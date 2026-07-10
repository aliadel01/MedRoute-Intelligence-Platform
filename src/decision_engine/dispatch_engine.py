# ════════════════════════════════════════════════════════════════
# MedRoute Dispatch Engine — Spark Structured Streaming Pipeline
# ════════════════════════════════════════════════════════════════
# Flow:
#   incident_stream (Kafka)
#   → parse JSON
#   → mapInPandas: PostGIS + OSRM Table (Fast Workers Iterators)
#   → foreachBatch → Sinks: [Kafka: dispatched_routes] + [Kafka: DLQ] + [Postgres]
# ════════════════════════════════════════════════════════════════
from __future__ import annotations

import json
import logging
import os
import sys
import polyline

import pandas as pd
import psycopg2
import psycopg2.pool
import requests

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json, lit, struct, to_json, when
from pyspark.sql.types import (
    ArrayType, DoubleType, IntegerType, StringType,
    StructField, StructType, TimestampType,
)

# ────────────────────────────────────────────────────────────────
# Stream & Logging Configuration (Docker Optimized via stdout)
# ────────────────────────────────────────────────────────────────

_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
_console_handler = logging.StreamHandler(sys.stdout)
_console_handler.setFormatter(_formatter)

# Force unbuffered stream behaviors for real-time Docker tracking
sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

logging.getLogger().setLevel(logging.INFO)
logging.getLogger().addHandler(_console_handler)

logger      = logging.getLogger("MedRoute.DispatchEngine")
_pg_log     = logging.getLogger("MedRoute.Worker.PostGIS")
_osrm_t_log = logging.getLogger("MedRoute.Worker.OSRM.Table")
_osrm_r_log = logging.getLogger("MedRoute.Worker.OSRM.Route")
_engine_log = logging.getLogger("MedRoute.Worker.DecisionEngine")
_sink_log   = logging.getLogger("MedRoute.Worker.Sink")

logger.info("Logging initialised via Standard Output (stdout).")

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

VALHALLA_URL      = "http://valhalla:8002"
VALHALLA_TIMEOUT  = 5

SEARCH_RADIUS_METERS = 30_000  
REQUIRED_BEDS = {1: 1, 2: 3, 3: 5, 4: 10}

HOSPITAL_TABLE = os.getenv("HOSPITAL_TABLE", "hospitals_olap")

# ────────────────────────────────────────────────────────────────
# Spark session
# ────────────────────────────────────────────────────────────────

logger.info("Initialising SparkSession with Kafka packages...")
try:
    spark = ( SparkSession.builder 
        .appName("MedRoute.DispatchEngine") 
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") 
        .config("spark.driver.extraClassPath", "/root/.ivy2/jars/*") 
        .config("spark.executor.extraClassPath", "/root/.ivy2/jars/*") 
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession ready.")
except Exception as e:
    logger.error("Failed to build or initialize SparkSession runtime component.", exc_info=True)
    sys.stdout.flush()
    sys.exit(1)

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
    StructField("incident_id",             StringType(),    True),
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
# Connection Pools Lazy Singletons (Worker Context)
# ────────────────────────────────────────────────────────────────

_db_pool      = None
_osrm_session = None

def _get_db_pool() -> psycopg2.pool.ThreadedConnectionPool:
    global _db_pool
    if _db_pool is None:
        _db_pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=2, maxconn=10, **POSTGRES_CONFIG
        )
        _pg_log.info("ThreadedConnectionPool initialised on Worker.")
    return _db_pool

def _get_osrm_session() -> requests.Session:
    global _osrm_session
    if _osrm_session is None:
        _osrm_session = requests.Session()
        _osrm_session.headers.update({"Connection": "keep-alive"})
        _osrm_t_log.info("OSRM HTTP session initialised on Worker.")
    return _osrm_session

# ────────────────────────────────────────────────────────────────
# Processing Step Helpers
# ────────────────────────────────────────────────────────────────

def _query_nearest_hospitals(db_pool, lon: float, lat: float) -> list[dict]:
    conn = None
    try:
        conn = db_pool.getconn()
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT name, latitude, longitude, COALESCE(icu_beds, 0)
                FROM {HOSPITAL_TABLE}
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
        _pg_log.error("PostGIS query failed internally on partition thread worker.", exc_info=True)
        return []
    finally:
        if conn:
            db_pool.putconn(conn)

def _enrich_with_valhalla(session, inc_lat, inc_lon, hospitals: list[dict]) -> list[dict]:
    """
    Call Valhalla /sources_to_targets (matrix API) to get travel times
    from the incident to all candidate hospitals simultaneously.
    Uses live traffic automatically — no extra config needed.
    """
    sources = [{"lat": inc_lat, "lon": inc_lon}]
    targets = [{"lat": h["lat"], "lon": h["lon"]} for h in hospitals]

    payload = {
        "sources": sources,
        "targets": targets,
        "costing": "auto",             # auto = car routing
        "costing_options": {
            "auto": {
                "use_traffic": 1.0     # 1.0 = fully use live traffic speeds
            }
        },
        "units": "kilometers"
    }

    try:
        resp = session.post(
            f"{VALHALLA_URL}/sources_to_targets",
            json=payload,
            timeout=VALHALLA_TIMEOUT,
        )
        if resp.status_code == 200:
            data = resp.json()
            # sources_to_targets returns: {"sources_to_targets": [[{time, distance}, ...]]}
            matrix = data["sources_to_targets"][0]   # one row per source (just our incident)

            for i, h in enumerate(hospitals):
                cell = matrix[i] if i < len(matrix) else {}
                # time is in seconds, distance is in km
                h["duration_sec"]    = float(cell.get("time",     -1) or -1)
                h["distance_meters"] = float(cell.get("distance", -1) or -1) * 1000
            return hospitals

        _osrm_t_log.error("Valhalla matrix HTTP %d.", resp.status_code)

    except Exception:
        _osrm_t_log.error("Valhalla matrix request failed.", exc_info=True)

    # Fallback — sentinel values
    for h in hospitals:
        h.setdefault("duration_sec",    -1.0)
        h.setdefault("distance_meters", -1.0)
    return hospitals

def _fetch_route_geometry(session, inc_lat, inc_lon, hospital: dict) -> str:
    """
    Call Valhalla /route and decode the returned Polyline6 string 
    into a GeoJSON LineString for Grafana compatibility.
    """
    payload = {
        "locations": [
            {"lat": inc_lat, "lon": inc_lon},
            {"lat": hospital["lat"], "lon": hospital["lon"]}
        ],
        "costing": "auto",
        "costing_options": {
            "auto": {"use_traffic": 1.0}
        },
        "units": "kilometers"
    }

    try:
        resp = session.post(
            f"{VALHALLA_URL}/route",
            json=payload,
            timeout=VALHALLA_TIMEOUT,
        )
        if resp.status_code == 200:
            data = resp.json()
            legs = data.get("trip", {}).get("legs", [])
            if legs:
                shape = legs[0].get("shape")
                
                if shape and isinstance(shape, str):
                    
                    decoded_coords = polyline.decode(shape, precision=6)
                
                    geojson_coords = [[pt[1], pt[0]] for pt in decoded_coords]
                    
                    return json.dumps({
                        "type": "LineString",
                        "coordinates": geojson_coords
                    })
                    
        else:
            _osrm_r_log.error("Valhalla route HTTP %d. Response: %s", resp.status_code, resp.text)

    except Exception:
        _osrm_r_log.error("Valhalla route request failed.", exc_info=True)

    return "Route Not Found"

def _select_best_hospital(severity: int, hospitals: list[dict]) -> dict | None:
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

# ────────────────────────────────────────────────────────────────
# Vectorized Batch Iterator Execution (mapInPandas Layout)
# ────────────────────────────────────────────────────────────────

def process_batch(iterator):
    try:
        db_pool = _get_db_pool()
        session = _get_osrm_session()
    except Exception:
        logging.getLogger("MedRoute.Worker.Global").error("Failed to initialize external services connections.", exc_info=True)
        for pdf in iterator:
            yield pd.DataFrame(columns=[f.name for f in output_schema.fields])
        return
    
    for pdf in iterator:
        if pdf.empty:
            yield pd.DataFrame(columns=[f.name for f in output_schema.fields])
            continue

        try:
            results = []
            for _, row in pdf.iterrows():
                lat      = row["Start_Lat"]
                lon      = row["Start_Lng"]
                severity = int(row["Severity"])

                hospitals = _query_nearest_hospitals(db_pool, lon, lat)
                if hospitals:
                    hospitals =  _enrich_with_valhalla(session, lat, lon, hospitals)

                best = _select_best_hospital(severity, hospitals)
                route_geometry = _fetch_route_geometry(session, lat, lon, best) if best else "Route Not Found"

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

            yield pd.DataFrame(results)

        except Exception:
            logging.getLogger("MedRoute.Worker.Global").error("Critical failure during mapInPandas processing iteration.", exc_info=True)
            yield pd.DataFrame(columns=[f.name for f in output_schema.fields])

# ────────────────────────────────────────────────────────────────
# foreachBatch Multi-Sink Coordinator (Driver Context Execution)
# ────────────────────────────────────────────────────────────────

def write_to_sinks(batch_df, batch_id):
    try:
        if batch_df.isEmpty():
            _sink_log.info("Batch %d empty — skipping downstream replication.", batch_id)
            return

        batch_df.persist()

        # ────────────────────────────────────────────────────────────
        # Split Data: Valid Routes vs DLQ (No Hospital OR Route Failed)
        # ────────────────────────────────────────────────────────────
        # Valid records must have a target hospital AND a successfully computed geometry
        valid_df = batch_df.filter(
            (col("target_hospital_name").isNotNull()) & 
            (col("route_geometry") != "Route Not Found")
        )
        
        # DLQ records: Either hospital lookup breached capacity, or Valhalla routing failed
        dlq_df = batch_df.filter(
            (col("target_hospital_name").isNull()) | 
            (col("route_geometry") == "Route Not Found")
        )

        # ────────────────────────────────────────────────────────────
        # Sink Execution: Valid Routes
        # ────────────────────────────────────────────────────────────
        if not valid_df.isEmpty():
            # 1. Write to PostgreSQL
            valid_df.write \
                .mode("append") \
                .jdbc(url=JDBC_URL, table=PG_TABLE, properties=JDBC_PROPS)
            _sink_log.info("Batch %d → Postgres '%s' ✓", batch_id, PG_TABLE)

            # 2. Write to Production Kafka Topic
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

        # ────────────────────────────────────────────────────────────
        # Sink Execution: Dead Letter Queue (DLQ)
        # ────────────────────────────────────────────────────────────
        if not dlq_df.isEmpty():
            
            # Dynamically attach the proper structural reason for routing to DLQ
            dlq_enriched_df = dlq_df.withColumn(
                "dlq_reason",
                when(col("target_hospital_name").isNull(), "No hospital found within search radius / capacity breach")
                .otherwise("Valhalla engine failed to resolve road network route geometry")
            ).withColumn("dlq_timestamp", current_timestamp())

            (
                dlq_enriched_df
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
            _sink_log.warning("Batch %d → DLQ '%s' (%d failed records routed) ✓", 
                             batch_id, KAFKA_DLQ_TOPIC, dlq_df.count())

    except Exception as exc:
        _sink_log.error("Batch %d sink runtime writing execution error encountered: %s", batch_id, exc, exc_info=True)
    finally:
        try:
            batch_df.unpersist()
        except Exception:
            pass

# ────────────────────────────────────────────────────────────────
# Execution Lifecycle
# ────────────────────────────────────────────────────────────────

try:
    logger.info("Subscribing to '%s' at %s.", KAFKA_IN_TOPIC, KAFKA_BOOTSTRAP)

    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe",            KAFKA_IN_TOPIC)
        .option("startingOffsets",      "latest")
        .option("maxOffsetsPerTrigger", 100)
        .option("failOnDataLoss",       "false")
        .load()
    )

    parsed_stream = (
        raw_stream
        .withColumn("value_str",   col("value").cast("string"))
        .withColumn("incident_data", from_json(col("value_str"), incident_schema))
        .select("incident_data.*", "timestamp")
    )

    final_df = parsed_stream.mapInPandas(process_batch, schema=output_schema)

    logger.info("Initializing multi-sink streaming execution context...")
    dispatch_query = (
        final_df.writeStream
        .foreachBatch(write_to_sinks)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .start()
    )

    logger.info("MedRoute Emergency Dispatch Engine is live and actively streaming.")
    dispatch_query.awaitTermination()

except Exception as global_exc:
    logger.critical("Fatal crash in streaming lifecycle engine.", exc_info=global_exc)
    sys.stdout.flush()
    sys.exit(1)