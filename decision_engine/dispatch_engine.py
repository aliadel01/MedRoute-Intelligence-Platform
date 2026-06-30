# ════════════════════════════════════════════════════════════════
# MedRoute Dispatch Engine — Spark Structured Streaming Pipeline
# ════════════════════════════════════════════════════════════════
# Flow:
#   incident_stream (Kafka) → parse → PostGIS nearest hospitals
#   → OSRM travel matrix → severity-based selection
#   → dispatched_routes (Kafka)
# ════════════════════════════════════════════════════════════════

import requests
import psycopg2
from psycopg2 import pool

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf, to_json, struct
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType, TimestampType,
    ArrayType,
)


# ────────────────────────────────────────────────────────────────
# Configuration
# ────────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP_SERVERS = "redpanda-0:29092"
KAFKA_INPUT_TOPIC       = "incident_stream"
KAFKA_OUTPUT_TOPIC      = "dispatched_routes"
KAFKA_CHECKPOINT_PATH   = "/tmp/spark_checkpoint/dispatched_routes"

POSTGRES_CONFIG = {
    "user":     "postgres",
    "password": "medroute_pass",
    "host":     "postgres",
    "port":     "5432",
    "database": "medroute_db",
}

OSRM_URL = "http://osrm:5000/table/v1/driving"
OSRM_TIMEOUT_SEC = 3

SEARCH_RADIUS_METERS = 10_000   # 10km candidate search radius

# Minimum ICU beds required per severity level
REQUIRED_BEDS_BY_SEVERITY = {
    1: 1,
    2: 3,
    3: 5,
    4: 10,
}


# ────────────────────────────────────────────────────────────────
# Spark session
# ────────────────────────────────────────────────────────────────

spark = (
    SparkSession.builder
    .appName("MedRoute-DispatchEngine")
    .master("local[*]")
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
print("SparkSession created successfully — ready for MedRoute pipeline.")


# ────────────────────────────────────────────────────────────────
# Schemas
# ────────────────────────────────────────────────────────────────

# Incoming incident event from incident_stream
incident_schema = StructType([
    StructField("ID",          StringType(),    nullable=False),
    StructField("Severity",    IntegerType(),   nullable=False),
    StructField("Start_Time",  TimestampType(), nullable=False),
    StructField("Start_Lat",   DoubleType(),    nullable=False),
    StructField("Start_Lng",   DoubleType(),    nullable=False),
    StructField("Description", StringType(),    nullable=False),
])

# Raw hospital candidate from PostGIS query
hospital_struct_type = StructType([
    StructField("name",        StringType(),  True),
    StructField("lat",         DoubleType(),  True),
    StructField("lon",         DoubleType(),  True),
    StructField("icu_beds",    IntegerType(), True),
])

# Hospital candidate after OSRM enriches it with travel metrics
enriched_hospital_struct = StructType([
    StructField("name",            StringType(),  True),
    StructField("lat",             DoubleType(),  True),
    StructField("lon",             DoubleType(),  True),
    StructField("icu_beds",        IntegerType(), True),
    StructField("duration_sec",    DoubleType(),  True),
    StructField("distance_meters", DoubleType(),  True),
])

list_hospital_struct_type = ArrayType(hospital_struct_type)
osrm_return_type          = ArrayType(enriched_hospital_struct)


# ────────────────────────────────────────────────────────────────
# Step 1 — PostGIS: nearest hospitals
# ────────────────────────────────────────────────────────────────

_db_pool = None  # lazily initialized per Spark worker


def _get_db_pool():
    """Create the connection pool once per worker process."""
    global _db_pool
    if _db_pool is None:
        _db_pool = psycopg2.pool.SimpleConnectionPool(
            minconn=1, maxconn=10, **POSTGRES_CONFIG
        )
    return _db_pool


def get_nearest_hospitals(lat, lon, radius_meters=SEARCH_RADIUS_METERS):
    """
    Query PostGIS for the 5 nearest hospitals to (lat, lon) within radius_meters.
    Returns a list of dicts matching hospital_struct_type.
    """
    if lat is None or lon is None:
        return []

    try:
        db_pool = _get_db_pool()
    except Exception:
        return []

    conn = None
    try:
        conn = db_pool.getconn()
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT name, latitude, longitude, COALESCE(icu_beds, 0)
                FROM hospitals
                WHERE ST_DWithin(
                    geom,
                    ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography,
                    %s
                )
                ORDER BY ST_Distance(
                    geom,
                    ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography
                )
                LIMIT 5;
                """,
                (lon, lat, radius_meters, lon, lat),
            )
            rows = cursor.fetchall()

        return [
            {
                "name":        str(row[0]),
                "lat":         float(row[1]),
                "lon":         float(row[2]),
                "icu_beds":    int(row[3]),
            }
            for row in rows
        ]

    except Exception:
        return []
    finally:
        if conn is not None:
            db_pool.putconn(conn)


# ────────────────────────────────────────────────────────────────
# Step 2 — OSRM: travel time / distance matrix
# ────────────────────────────────────────────────────────────────

def get_osrm_metrics(incident_lat, incident_lon, hospitals_list):
    """
    Call OSRM's table endpoint once to get travel duration and distance
    from the incident location to every candidate hospital.

    hospitals_list items may be PySpark Row objects (from the UDF chain),
    so all field access uses getattr() rather than dict-style .get().
    """
    if incident_lat is None or incident_lon is None or not hospitals_list:
        return []

    coordinates = [f"{incident_lon},{incident_lat}"]
    for hospital in hospitals_list:
        if hospital is None:
            continue
        lon_val = getattr(hospital, "lon", None)
        lat_val = getattr(hospital, "lat", None)
        if lon_val is not None and lat_val is not None:
            coordinates.append(f"{lon_val},{lat_val}")

    coordinates_path = ";".join(coordinates)
    dest_indices = ";".join(str(i) for i in range(1, len(hospitals_list) + 1))

    params = {
        "sources": "0",
        "destinations": dest_indices,
        "annotations": "duration,distance",
    }

    try:
        response = requests.get(
            f"{OSRM_URL}/{coordinates_path}",
            params=params,
            timeout=OSRM_TIMEOUT_SEC,
        )
        if response.status_code == 200:
            data = response.json()
            durations = data["durations"][0]
            distances = data["distances"][0]

            return [
                {
                    "name":            getattr(h, "name", "Unknown"),
                    "lat":             getattr(h, "lat", 0.0),
                    "lon":             getattr(h, "lon", 0.0),
                    "icu_beds":        getattr(h, "icu_beds", 0),
                    "duration_sec":    float(durations[idx]) if durations[idx] is not None else -1.0,
                    "distance_meters": float(distances[idx]) if distances[idx] is not None else -1.0,
                }
                for idx, h in enumerate(hospitals_list)
                if h is not None
            ]
    except Exception:
        pass

    # OSRM unreachable or request failed — return candidates with sentinel metrics
    # rather than dropping them, so downstream selection still has options.
    return [
        {
            "name":            getattr(h, "name", "Unknown"),
            "lat":             getattr(h, "lat", 0.0),
            "lon":             getattr(h, "lon", 0.0),
            "icu_beds":        getattr(h, "icu_beds", 0),
            "duration_sec":    -1.0,
            "distance_meters": -1.0,
        }
        for h in hospitals_list if h is not None
    ]


# ────────────────────────────────────────────────────────────────
# Step 3 — Selection: best hospital by severity + travel time
# ────────────────────────────────────────────────────────────────

def get_best_hospital(severity, hospitals_list):
    """
    Sort candidates by travel duration ascending, then return the fastest
    one that meets the minimum ICU bed requirement for this severity.
    Falls back to the closest hospital overall if none meet the requirement.
    """
    if not hospitals_list:
        return None

    sorted_hospitals = sorted(
        hospitals_list,
        key=lambda h: getattr(h, "duration_sec", None) or float("inf"),
    )

    required_beds = REQUIRED_BEDS_BY_SEVERITY.get(severity, 1)

    for hospital in sorted_hospitals:
        if hospital is None:
            continue
        if getattr(hospital, "icu_beds", 0) >= required_beds:
            return hospital

    return sorted_hospitals[0]


# ────────────────────────────────────────────────────────────────
# UDF registration
# ────────────────────────────────────────────────────────────────

get_hospitals_udf     = udf(get_nearest_hospitals, list_hospital_struct_type)
get_osrm_metrics_udf  = udf(get_osrm_metrics, osrm_return_type)
get_best_hospital_udf = udf(get_best_hospital, enriched_hospital_struct)


# ────────────────────────────────────────────────────────────────
# Pipeline — read, enrich, select, write
# ────────────────────────────────────────────────────────────────

# 1. Read raw events from Kafka
raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_INPUT_TOPIC)
    .option("startingOffsets", "latest")
    .option("maxOffsetsPerTrigger", 100)
    .load()
)

# 2. Parse the JSON payload
parsed_stream = (
    raw_stream
    .withColumn("value_str", col("value").cast("string"))
    .withColumn("incident_data", from_json(col("value_str"), incident_schema))
    .select("incident_data.*", "timestamp")
)

# 3. Look up nearest hospitals via PostGIS
enriched_df = parsed_stream.withColumn(
    "nearest_hospitals_array",
    get_hospitals_udf(col("Start_Lat"), col("Start_Lng")),
)

# 4. Enrich candidates with OSRM travel metrics
routing_df = enriched_df.withColumn(
    "hospitals_with_routing",
    get_osrm_metrics_udf(col("Start_Lat"), col("Start_Lng"), col("nearest_hospitals_array")),
)

# 5. Select the best hospital for this incident's severity
matched_df = routing_df.withColumn(
    "best_hospital_match",
    get_best_hospital_udf(col("Severity"), col("hospitals_with_routing")),
)

# 6. Flatten into the final output shape
final_df = matched_df.select(
    col("ID").alias("incident_id"),
    col("Severity").alias("severity"),
    col("Start_Time").alias("incident_start_time"),
    col("Start_Lat").alias("incident_lat"),
    col("Start_Lng").alias("incident_lon"),
    col("Description").alias("incident_description"),
    col("timestamp").alias("kafka_timestamp"),
    col("best_hospital_match.name").alias("target_hospital_name"),
    col("best_hospital_match.lat").alias("target_hospital_lat"),
    col("best_hospital_match.lon").alias("target_hospital_lon"),
    col("best_hospital_match.icu_beds").alias("available_icu_beds"),
    col("best_hospital_match.duration_sec").alias("travel_duration_seconds"),
    col("best_hospital_match.distance_meters").alias("travel_distance_meters"),
)

# 7. Build the Kafka output payload — key + JSON value
kafka_payload_df = final_df.select(
    col("incident_id").cast("string").alias("key"),
    to_json(struct(
        col("incident_id"),
        col("severity"),
        col("incident_start_time"),
        col("incident_lat"),
        col("incident_lon"),
        col("incident_description"),
        col("kafka_timestamp"),
        col("target_hospital_name"),
        col("target_hospital_lat"),
        col("target_hospital_lon"),
        col("available_icu_beds"),
        col("travel_duration_seconds"),
        col("travel_distance_meters"),
    )).alias("value"),
)

# 8. Write dispatch decisions back to Kafka
dispatch_query = (
    kafka_payload_df.writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("topic", KAFKA_OUTPUT_TOPIC)
    .option("checkpointLocation", KAFKA_CHECKPOINT_PATH)
    .outputMode("append")
    .start()
)

dispatch_query.awaitTermination()