# ════════════════════════════════════════════════════════════════
# MedRoute Dispatch Engine — Spark Structured Streaming Pipeline
# ════════════════════════════════════════════════════════════════
# Flow:
#   incident_stream (Kafka)
#   → parse JSON
#   → mapInPandas: PostGIS + OSRM table + severity selection + OSRM route
#   → foreachBatch → [Kafka: dispatched_routes] + [Kafka: DLQ] + [Postgres]
# ════════════════════════════════════════════════════════════════

import logging
import os
import sys

import pandas as pd
import psycopg2
import psycopg2.pool
import requests

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json, lit, struct, to_json
from pyspark.sql.types import (
    ArrayType, DoubleType, IntegerType, StringType,
    StructField, StructType, TimestampType,
)


# ────────────────────────────────────────────────────────────────
# Logging
# ────────────────────────────────────────────────────────────────

LOG_FILE_PATH = "medroute_engine.log"

_formatter       = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
_file_handler    = logging.FileHandler(LOG_FILE_PATH, mode="a", encoding="utf-8")
_console_handler = logging.StreamHandler(sys.stdout)
_file_handler.setFormatter(_formatter)
_console_handler.setFormatter(_formatter)

logging.getLogger().setLevel(logging.INFO)
logging.getLogger().addHandler(_file_handler)
logging.getLogger().addHandler(_console_handler)

# Module-level loggers — defined once, never recreated per row
logger          = logging.getLogger("MedRoute.DispatchEngine")
_pg_log         = logging.getLogger("MedRoute.Worker.PostGIS")
_osrm_t_log     = logging.getLogger("MedRoute.Worker.OSRM.Table")
_osrm_r_log     = logging.getLogger("MedRoute.Worker.OSRM.Route")
_engine_log     = logging.getLogger("MedRoute.Worker.DecisionEngine")
_sink_log       = logging.getLogger("MedRoute.Worker.Sink")

logger.info("Logging initialised — file: %s", os.path.abspath(LOG_FILE_PATH))


# ────────────────────────────────────────────────────────────────
# Configuration
# ────────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP  = "redpanda-0:29092"
KAFKA_IN_TOPIC   = "incident_stream"
KAFKA_OUT_TOPIC  = "dispatched_routes"
KAFKA_DLQ_TOPIC  = "dispatched_routes_dlq"
CHECKPOINT_PATH  = "/opt/spark/checkpoints/dispatched_routes"

POSTGRES_CONFIG = {
    "user":     "postgres",
    "password": "medroute_pass",
    "host":     "medroute_postgres",
    "port":     "5432",
    "database": "medroute_db",
}

JDBC_URL = (
    f"jdbc:postgresql://{POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}"
    f"/{POSTGRES_CONFIG['database']}"
)
JDBC_PROPS = {
    "user":     POSTGRES_CONFIG["user"],
    "password": POSTGRES_CONFIG["password"],
    "driver":   "org.postgresql.Driver",
}
PG_TABLE = "dispatched_routes"

OSRM_TABLE_URL   = "http://osrm:5000/table/v1/driving"
OSRM_ROUTE_URL   = "http://osrm:5000/route/v1/driving"
OSRM_TIMEOUT_SEC = 3

SEARCH_RADIUS_METERS = 30_000   # 30 km

# Minimum ICU beds required per severity level
REQUIRED_BEDS = {1: 1, 2: 3, 3: 5, 4: 10}


# ────────────────────────────────────────────────────────────────
# Spark session
# ────────────────────────────────────────────────────────────────

logger.info("Initialising SparkSession...")

spark = (
    SparkSession.builder
    .appName("MedRoute.DispatchEngine")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
        "org.postgresql:postgresql:42.7.1",
    )
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
logger.info("SparkSession ready.")


# ────────────────────────────────────────────────────────────────
# Schemas
# ────────────────────────────────────────────────────────────────

incident_schema = StructType([
    StructField("ID",          StringType(),    nullable=False),
    StructField("Severity",    IntegerType(),   nullable=False),
    StructField("Start_Time",  TimestampType(), nullable=False),
    StructField("Start_Lat",   DoubleType(),    nullable=False),
    StructField("Start_Lng",   DoubleType(),    nullable=False),
    StructField("Description", StringType(),    nullable=False),
])

output_schema = StructType([
    StructField("incident_id",           StringType(),    True),
    StructField("severity",              IntegerType(),   True),
    StructField("incident_start_time",   TimestampType(), True),
    StructField("incident_lat",          DoubleType(),    True),
    StructField("incident_lon",          DoubleType(),    True),
    StructField("incident_description",  StringType(),    True),
    StructField("kafka_timestamp",       TimestampType(), True),
    StructField("target_hospital_name",  StringType(),    True),
    StructField("target_hospital_lat",   DoubleType(),    True),
    StructField("target_hospital_lon",   DoubleType(),    True),
    StructField("available_icu_beds",    IntegerType(),   True),
    StructField("travel_duration_seconds", DoubleType(), True),
    StructField("travel_distance_meters",  DoubleType(), True),
    StructField("route_geometry",        StringType(),    True),
])


# ────────────────────────────────────────────────────────────────
# Lazy singletons — created once per Python worker process
# ────────────────────────────────────────────────────────────────

_db_pool      = None
_osrm_session = None


def _get_db_pool() -> psycopg2.pool.ThreadedConnectionPool:
    """ThreadedConnectionPool — safe for Spark local[*] multi-threading."""
    global _db_pool
    if _db_pool is None:
        _db_pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=2, maxconn=10, **POSTGRES_CONFIG
        )
        _pg_log.info("ThreadedConnectionPool initialised.")
    return _db_pool


def _get_osrm_session() -> requests.Session:
    """Persistent HTTP session — reuses TCP connections across OSRM calls."""
    global _osrm_session
    if _osrm_session is None:
        _osrm_session = requests.Session()
        _osrm_session.headers.update({"Connection": "keep-alive"})
        _osrm_t_log.info("OSRM HTTP session initialised.")
    return _osrm_session


# ────────────────────────────────────────────────────────────────
# Step helpers — called per incident inside mapInPandas
# ────────────────────────────────────────────────────────────────

def _query_nearest_hospitals(db_pool, lon: float, lat: float) -> list[dict]:
    """
    PostGIS spatial query — returns up to 5 nearest hospitals.
    Uses ST_DWithin for filtering and <-> KNN operator for ordering
    so distance is computed only once against the spatial index.
    """
    conn = None
    try:
        conn = db_pool.getconn()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT name, latitude, longitude, COALESCE(icu_beds, 0)
                FROM hospitals
                WHERE ST_DWithin(
                    geom,
                    ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography,
                    %s
                )
                ORDER BY geom <-> ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography
                LIMIT 5;
                """,
                (lon, lat, SEARCH_RADIUS_METERS, lon, lat),
            )
            return [
                {"name": r[0], "lat": float(r[1]), "lon": float(r[2]), "icu_beds": int(r[3])}
                for r in cur.fetchall()         
            ]
    except Exception:
        _pg_log.error("PostGIS query failed.", exc_info=True)
        return []
    finally:
        if conn:
            db_pool.putconn(conn)


def _enrich_with_osrm_table(session, inc_lon, inc_lat, hospitals: list[dict]) -> list[dict]:
    """
    Single OSRM table request → attaches duration_sec and distance_meters
    to each hospital dict in-place. Returns the enriched list.
    """
    coords = [f"{inc_lon},{inc_lat}"] + [f"{h['lon']},{h['lat']}" for h in hospitals]
    params = {
        "sources":      "0",
        "destinations": ";".join(str(i) for i in range(1, len(hospitals) + 1)),
        "annotations":  "duration,distance",
    }
    try:
        resp = session.get(
            f"{OSRM_TABLE_URL}/{';'.join(coords)}",
            params=params,
            timeout=OSRM_TIMEOUT_SEC,
        )
        if resp.status_code == 200:
            data      = resp.json()
            durations = data["durations"][0]
            distances = data["distances"][0]
            for i, h in enumerate(hospitals):
                h["duration_sec"]    = float(durations[i]) if durations[i] is not None else -1.0
                h["distance_meters"] = float(distances[i]) if distances[i] is not None else -1.0
            return hospitals
        _osrm_t_log.error("OSRM table HTTP %d.", resp.status_code)
    except Exception:
        _osrm_t_log.error("OSRM table request failed.", exc_info=True)

    # Fallback — sentinel values so selection still runs
    for h in hospitals:
        h.setdefault("duration_sec",    -1.0)
        h.setdefault("distance_meters", -1.0)
    return hospitals


def _select_best_hospital(severity: int, hospitals: list[dict]) -> dict | None:
    """
    Sort by travel duration ascending (sentinels last), then return the
    fastest hospital that meets the ICU bed requirement for this severity.
    Falls back to the closest if none qualify.
    """
    if not hospitals:
        return None

    sorted_h      = sorted(hospitals, key=lambda h: h["duration_sec"] if h["duration_sec"] >= 0 else float("inf"))
    required_beds = REQUIRED_BEDS.get(severity, 1)

    for h in sorted_h:
        if h["icu_beds"] >= required_beds:
            _engine_log.info(
                "Selected '%s' — beds: %d, ETA: %.0fs (severity %d, needs %d beds).",
                h["name"], h["icu_beds"], h["duration_sec"], severity, required_beds,
            )
            return h

    _engine_log.warning(
        "No hospital met %d-bed requirement (severity %d). Fallback: '%s'.",
        required_beds, severity, sorted_h[0]["name"],
    )
    return sorted_h[0]


def _fetch_route_geometry(session, inc_lon, inc_lat, hospital: dict) -> str:
    """
    OSRM route endpoint for the single selected hospital.
    Returns a GeoJSON geometry string (LineString) for Grafana.
    """
    params = {"overview": "full", "geometries": "geojson"}
    try:
        resp = session.get(
            f"{OSRM_ROUTE_URL}/{inc_lon},{inc_lat};{hospital['lon']},{hospital['lat']}",
            params=params,
            timeout=OSRM_TIMEOUT_SEC,
        )
        if resp.status_code == 200:
            data = resp.json()
            if data.get("routes"):
                return str(data["routes"][0]["geometry"])
        _osrm_r_log.error("OSRM route HTTP %d.", resp.status_code)
    except Exception:
        _osrm_r_log.error("OSRM route request failed.", exc_info=True)
    return "Route Not Found"


# ────────────────────────────────────────────────────────────────
# mapInPandas batch processor
# ────────────────────────────────────────────────────────────────

def process_batch(pdf: pd.DataFrame) -> pd.DataFrame:
    """
    Called by Spark once per partition per micro-batch.
    Initialises shared resources once, then processes every incident
    in the partition sequentially inside Python — avoiding per-row
    JVM ↔ Python serialisation overhead of chained UDFs.
    """
    if pdf.empty:
        return pd.DataFrame(columns=[f.name for f in output_schema.fields])

    db_pool = _get_db_pool()
    session = _get_osrm_session()
    results = []

    for _, row in pdf.iterrows():
        lat      = row["Start_Lat"]
        lon      = row["Start_Lng"]
        severity = int(row["Severity"])

        # 1 — PostGIS: nearest hospitals
        hospitals = _query_nearest_hospitals(db_pool, lon, lat)

        # 2 — OSRM table: travel time + distance for all candidates at once
        if hospitals:
            hospitals = _enrich_with_osrm_table(session, lon, lat, hospitals)

        # 3 — Decision: best hospital by severity + travel time
        best = _select_best_hospital(severity, hospitals)

        # 4 — OSRM route: road-level geometry for Grafana map
        route_geometry = _fetch_route_geometry(session, lon, lat, best) if best else "Route Not Found"

        results.append({
            "incident_id":           row["ID"],
            "severity":              severity,
            "incident_start_time":   row["Start_Time"],
            "incident_lat":          lat,
            "incident_lon":          lon,
            "incident_description":  row["Description"],
            "kafka_timestamp":       row["timestamp"],
            "target_hospital_name":  best["name"]            if best else None,
            "target_hospital_lat":   best["lat"]             if best else None,
            "target_hospital_lon":   best["lon"]             if best else None,
            "available_icu_beds":    best["icu_beds"]        if best else None,
            "travel_duration_seconds": best["duration_sec"]  if best else None,
            "travel_distance_meters":  best["distance_meters"] if best else None,
            "route_geometry":        route_geometry,
        })

    return pd.DataFrame(results)


# ────────────────────────────────────────────────────────────────
# Streaming pipeline — read and transform
# ────────────────────────────────────────────────────────────────

logger.info("Subscribing to '%s' at %s.", KAFKA_IN_TOPIC, KAFKA_BOOTSTRAP)

raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe",            KAFKA_IN_TOPIC)
    .option("startingOffsets",      "latest")
    .option("maxOffsetsPerTrigger", 100)
    .option("failOnDataLoss",       "false")   # NOTE: set to true in production
    .load()
)

parsed_stream = (
    raw_stream
    .withColumn("value_str",     col("value").cast("string"))
    .withColumn("incident_data", from_json(col("value_str"), incident_schema))
    .select("incident_data.*", "timestamp")
)

final_df = parsed_stream.mapInPandas(process_batch, schema=output_schema)


# ────────────────────────────────────────────────────────────────
# foreachBatch — three sinks: Postgres + Kafka main + Kafka DLQ
# ────────────────────────────────────────────────────────────────

def write_to_sinks(batch_df, batch_id):
    """
    Called by Spark on every micro-batch.

    Valid records  → Postgres dispatched_routes table
                   → Kafka dispatched_routes topic
    Failed records → Kafka dispatched_routes_dlq topic

    batch_df is cached before the first write so Spark does not
    re-execute mapInPandas (PostGIS + OSRM calls) for each sink.
    """
    if batch_df.isEmpty():
        _sink_log.info("Batch %d empty — skipping.", batch_id)
        return

    batch_df.persist()

    try:
        valid_df = batch_df.filter(col("target_hospital_name").isNotNull())
        dlq_df   = batch_df.filter(col("target_hospital_name").isNull())

        # ── Sink 1 & 2: valid records ─────────────────────────────
        if not valid_df.isEmpty():
            # Postgres
            valid_df.write \
                .mode("append") \
                .jdbc(url=JDBC_URL, table=PG_TABLE, properties=JDBC_PROPS)
            _sink_log.info("Batch %d → Postgres '%s' ✓", batch_id, PG_TABLE)

            # Kafka main topic
            (
                valid_df
                .select(
                    col("incident_id").cast("string").alias("key"),
                    to_json(struct("*")).alias("value"),
                )
                .write
                .format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
                .option("topic", KAFKA_OUT_TOPIC)
                .save()
            )
            _sink_log.info("Batch %d → Kafka '%s' ✓", batch_id, KAFKA_OUT_TOPIC)

        # ── Sink 3: failed records → DLQ ─────────────────────────
        if not dlq_df.isEmpty():
            (
                dlq_df
                .withColumn("dlq_reason",    lit("No hospital found within search radius"))
                .withColumn("dlq_timestamp", current_timestamp())
                .select(
                    col("incident_id").cast("string").alias("key"),
                    to_json(struct("*")).alias("value"),
                )
                .write
                .format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
                .option("topic", KAFKA_DLQ_TOPIC)
                .save()
            )
            _sink_log.warning("Batch %d → DLQ '%s' (%d failed) ✓",
                              batch_id, KAFKA_DLQ_TOPIC, dlq_df.count())

    except Exception as exc:
        _sink_log.error("Batch %d sink error: %s", batch_id, exc, exc_info=True)
    finally:
        batch_df.unpersist()


# ────────────────────────────────────────────────────────────────
# Start
# ────────────────────────────────────────────────────────────────

logger.info("Starting dispatch query — checkpoint: %s", CHECKPOINT_PATH)

dispatch_query = (
    final_df.writeStream
    .foreachBatch(write_to_sinks)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(processingTime="2 seconds")
    .start()
)

logger.info("MedRoute Dispatch Engine is live.")
dispatch_query.awaitTermination()