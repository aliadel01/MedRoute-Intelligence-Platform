# ════════════════════════════════════════════════════════════════
# MedRoute Dispatch Engine — Spark Structured Streaming Pipeline
# ════════════════════════════════════════════════════════════════

import requests
import psycopg2
from psycopg2 import pool
import logging
import sys
import os
import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_json, struct, current_timestamp, lit
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType, TimestampType,
    ArrayType,
)

# ────────────────────────────────────────────────────────────────
# Logging Configuration
# ────────────────────────────────────────────────────────────────

LOG_FILE_PATH = "medroute_engine.log"
log_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")

file_handler = logging.FileHandler(LOG_FILE_PATH, mode="a", encoding="utf-8")
file_handler.setFormatter(log_formatter)

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(log_formatter)

root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)
root_logger.addHandler(file_handler)
root_logger.addHandler(console_handler)

logger = logging.getLogger("MedRoute.DispatchEngine")
logger.info(f"Logging initialized. Saved to: {os.path.abspath(LOG_FILE_PATH)}")

# [PERFORMANCE FIX] Define Loggers outside UDFs to avoid recreation overhead per row
_postgis_logger = logging.getLogger("MedRoute.Worker.PostGIS")
_osrm_table_logger = logging.getLogger("MedRoute.Worker.OSRM-Table")
_engine_logger = logging.getLogger("MedRoute.Worker.DecisionEngine")
_router_logger = logging.getLogger("MedRoute.Worker.OSRM-Router")

# ────────────────────────────────────────────────────────────────
# Configuration
# ────────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP_SERVERS = "redpanda-0:29092"
KAFKA_INPUT_TOPIC       = "incident_stream"
KAFKA_OUTPUT_TOPIC      = "dispatched_routes"
KAFKA_CHECKPOINT_PATH   = "/opt/spark/checkpoints/dispatched_routes"

POSTGRES_CONFIG = {
    "user":     "admin",
    "password": "12345678",
    "host":     "med_postgres",
    "port":     "5432",
    "database": "medroute_db",
}

OSRM_URL = "http://osrm:5000/table/v1/driving"
OSRM_TIMEOUT_SEC = 3
SEARCH_RADIUS_METERS = 10_000   # 10km candidate search radius

REQUIRED_BEDS_BY_SEVERITY = {1: 1, 2: 3, 3: 5, 4: 10}

# ────────────────────────────────────────────────────────────────
# Spark session
# ────────────────────────────────────────────────────────────────

logger.info("Initializing SparkSession with Kafka and PostgreSQL packages...")

spark = ( SparkSession.builder \
    .appName("MedRoute.DispatchEngine") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .config("spark.driver.extraClassPath", "/root/.ivy2/jars/*") \
    .config("spark.executor.extraClassPath", "/root/.ivy2/jars/*") \
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")    
logger.info("SparkSession created successfully.")

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

# ────────────────────────────────────────────────────────────────
# Connection Pools Lazy Initialization
# ────────────────────────────────────────────────────────────────

_db_pool = None  
_osrm_session = None  

def _get_db_pool():
    global _db_pool
    if _db_pool is None:
        try:
            _db_pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=2, maxconn=10, **POSTGRES_CONFIG
            )
            logging.getLogger("MedRoute.Worker.PostgreSQL").info("Database ThreadedConnectionPool initialized.")
        except Exception as e:
            logging.getLogger("MedRoute.Worker.PostgreSQL").error(f"Failed to initialize PostGIS DB pool: {str(e)}")
            raise e
    return _db_pool

def _get_osrm_session():
    global _osrm_session
    if _osrm_session is None:
        try:
            _osrm_session = requests.Session()
            _osrm_session.headers.update({"Connection": "keep-alive"})
            logging.getLogger("MedRoute.Worker.OSRM").info("OSRM session initialized.")
        except Exception as e:
            logging.getLogger("MedRoute.Worker.OSRM").error(f"Failed to initialize OSRM session: {str(e)}")
            raise e
    return _osrm_session

# ────────────────────────────────────────────────────────────────
# Vectorized Batch Execution (Replaces 4 Heavy Python UDFs)
# ────────────────────────────────────────────────────────────────

def process_gps_batch_vectorized(pdf: pd.DataFrame) -> pd.DataFrame:
    """
    Processes the entire micro-batch as a unified Pandas DataFrame.
    This executes within the Python Worker, eliminating row-by-row JVM serialization overhead.
    """
    if pdf.empty:
        return pdf

    results = []
    
    # Initialize shared connection pools once for the entire batch
    db_pool = _get_db_pool()
    session = _get_osrm_session()

    # Iterate through the rows efficiently inside Python context
    for _, row in pdf.iterrows():
        lat, lon, severity = row['Start_Lat'], row['Start_Lng'], row['Severity']
        
        # 1. Spatial Processing (PostGIS Lookup)
        nearest_hospitals = []
        if lat is not None and lon is not None:
            conn = None
            try:
                conn = db_pool.getconn()
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT name, latitude, longitude, COALESCE(icu_beds, 0)
                        FROM hospitals
                        WHERE ST_DWithin(geom, ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography, %s)
                        ORDER BY geom <-> ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography LIMIT 5;
                        """, (lon, lat, SEARCH_RADIUS_METERS, lon, lat)
                    )
                    nearest_hospitals = [
                        {"name": row[0], "lat": float(row[1]), "lon": float(row[2]), "icu_beds": int(row[3])}
                        for row in cursor.fetchall()
                    ]
            except Exception:
                _postgis_logger.error("PostGIS error inside optimized batch", exc_info=True)
            finally:
                if conn: 
                    db_pool.putconn(conn)

        # 2 & 3. Network Routing & Decision Match Engine
        best_hospital = None
        if nearest_hospitals:
            coordinates = [f"{lon},{lat}"] + [f"{h['lon']},{h['lat']}" for h in nearest_hospitals]
            coordinates_path = ";".join(coordinates)
            dest_indices = ";".join(str(i) for i in range(1, len(nearest_hospitals) + 1))
            params = {"sources": "0", "destinations": dest_indices, "annotations": "duration,distance"}
            
            try:
                response = session.get(f"{OSRM_URL}/{coordinates_path}", params=params, timeout=OSRM_TIMEOUT_SEC)
                if response.status_code == 200:
                    data = response.json()
                    durations = data["durations"][0]
                    distances = data["distances"][0]
                    
                    for idx, h in enumerate(nearest_hospitals):
                        h["duration_sec"] = float(durations[idx]) if durations[idx] is not None else -1.0
                        h["distance_meters"] = float(distances[idx]) if distances[idx] is not None else -1.0
                    
                    # Sort candidates by travel duration time
                    sorted_hospitals = sorted(nearest_hospitals, key=lambda x: x["duration_sec"] if x["duration_sec"] >= 0 else float("inf"))
                    required_beds = REQUIRED_BEDS_BY_SEVERITY.get(severity, 1)
                    
                    # Match candidate against ICU capacity requirement
                    for h in sorted_hospitals:
                        if h["icu_beds"] >= required_beds:
                            best_hospital = h
                            break
                    if not best_hospital and sorted_hospitals:
                        best_hospital = sorted_hospitals[0]
            except Exception:
                _osrm_table_logger.error("OSRM matrix call error inside optimized batch", exc_info=True)

        # 4. Geometry Generation (OSRM Router)
        route_geometry = "Route Not Found"
        if best_hospital:
            route_url = f"http://osrm:5000/route/v1/driving/{lon},{lat};{best_hospital['lon']},{best_hospital['lat']}"
            try:
                response = session.get(route_url, params={"overview": "full", "geometries": "geojson"}, timeout=2)
                if response.status_code == 200:
                    data = response.json()
                    if data.get("routes"):
                        route_geometry = str(data["routes"][0]["geometry"])
            except Exception:
                _router_logger.error("OSRM Routing error inside optimized batch", exc_info=True)

        # Append structured record matching final flattened layout
        results.append({
            "incident_id": row["ID"],
            "severity": severity,
            "incident_start_time": row["Start_Time"],
            "incident_lat": lat,
            "incident_lon": lon,
            "incident_description": row["Description"],
            "kafka_timestamp": row["timestamp"],
            "target_hospital_name": best_hospital["name"] if best_hospital else None,
            "target_hospital_lat": best_hospital["lat"] if best_hospital else None,
            "target_hospital_lon": best_hospital["lon"] if best_hospital else None,
            "available_icu_beds": best_hospital["icu_beds"] if best_hospital else None,
            "travel_duration_seconds": best_hospital["duration_sec"] if best_hospital else None,
            "travel_distance_meters": best_hospital["distance_meters"] if best_hospital else None,
            "route_geometry": route_geometry
        })

    return pd.DataFrame(results)

# ────────────────────────────────────────────────────────────────
# Output Schema Definition for mapInPandas Execution
# ────────────────────────────────────────────────────────────────
output_schema = StructType([
    StructField("incident_id", StringType(), True),
    StructField("severity", IntegerType(), True),
    StructField("incident_start_time", TimestampType(), True),
    StructField("incident_lat", DoubleType(), True),
    StructField("incident_lon", DoubleType(), True),
    StructField("incident_description", StringType(), True),
    StructField("kafka_timestamp", TimestampType(), True),
    StructField("target_hospital_name", StringType(), True),
    StructField("target_hospital_lat", DoubleType(), True),
    StructField("target_hospital_lon", DoubleType(), True),
    StructField("available_icu_beds", IntegerType(), True),
    StructField("travel_duration_seconds", DoubleType(), True),
    StructField("travel_distance_meters", DoubleType(), True),
    StructField("route_geometry", StringType(), True),
])

# ────────────────────────────────────────────────────────────────
# Pipeline Ingestion & Enrichment (Upstream)
# ────────────────────────────────────────────────────────────────

raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_INPUT_TOPIC)
    .option("startingOffsets", "latest")
    .option("maxOffsetsPerTrigger", 100)
    .option("failOnDataLoss", "false")
    .load()
)

parsed_stream = (
    raw_stream
    .withColumn("value_str", col("value").cast("string"))
    .withColumn("incident_data", from_json(col("value_str"), incident_schema))
    .select("incident_data.*", "timestamp")
)

# Convert micro-batch to Pandas vectors to avoid multi-UDF overhead
final_df = parsed_stream.mapInPandas(process_gps_batch_vectorized, schema=output_schema)

# ────────────────────────────────────────────────────────────────
# Unified Multi-Sink Configuration (`foreachBatch`)
# ────────────────────────────────────────────────────────────────

JDBC_URL = f"jdbc:postgresql://{POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}/{POSTGRES_CONFIG['database']}"
DB_PROPERTIES = {
    "user": POSTGRES_CONFIG["user"],
    "password": POSTGRES_CONFIG["password"],
    "driver": "org.postgresql.Driver"
}
DB_TABLE_NAME = "dispatched_routes"


def write_to_sinks_foreach_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    # Cache the batch DataFrame since it will be used for multiple filtering operations
    batch_df.persist()

    # 1. Separate valid records from failed ones (DLQ)
    # An incident is considered failed if no target hospital was found (target_hospital_name is NULL)
    valid_df = batch_df.filter(col("target_hospital_name").isNotNull())
    dlq_df = batch_df.filter(col("target_hospital_name").isNull())

    total_records = batch_df.count()
    valid_count = valid_df.count()
    dlq_count = total_records - valid_count

    try:
        # ---- First: Process Valid Records (PostgreSQL + Main Kafka Topic) ----
        if valid_count > 0:
            logger.info(f"Batch {batch_id}: Writing {valid_count} valid records to PostgreSQL.")
            valid_df.write \
                .mode("append") \
                .jdbc(url=JDBC_URL, table="dispatched_routes", properties=DB_PROPERTIES)

            logger.info(f"Batch {batch_id}: Streaming valid records to Main Kafka Topic.")
            kafka_main_payload = valid_df.select(
                col("incident_id").cast("string").alias("key"),
                to_json(struct("*")).alias("value")
            )
            
            kafka_main_payload.write \
                .format("kafka") \
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
                .option("topic", "dispatched_routes") \
                .save()

        # ---- Second: Process Failed Records (Route to DLQ Kafka Topic) ----
        if dlq_count > 0:
            logger.warning(f"Batch {batch_id}: {dlq_count} records failed. Routing to DLQ Topic...")
            
            # Enrich failed records with error context and processing timestamp for future auditing
            dlq_payload = dlq_df.withColumn("dlq_reason", lit("No target hospital found / NOT NULL constraint violation")) \
                                .withColumn("dlq_timestamp", current_timestamp()) \
                                .select(
                                    col("incident_id").cast("string").alias("key"),
                                    to_json(struct("*")).alias("value")
                                )
            
            dlq_payload.write \
                .format("kafka") \
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
                .option("topic", "dispatched_routes_dlq") \
                .save()

    except Exception as e:
        logger.error(f"Critical error in micro-batch {batch_id}: {str(e)}")
    finally:
        # Always unpersist to release memory cluster-wide and prevent leaks
        batch_df.unpersist()


# ────────────────────────────────────────────────────────────────
# Execution Initialization
# ────────────────────────────────────────────────────────────────
logger.info("Initializing multi-sink streaming query (Kafka & PostgreSQL)...")

dispatch_query = (
    final_df.writeStream
    .foreachBatch(write_to_sinks_foreach_batch)
    .option("checkpointLocation", KAFKA_CHECKPOINT_PATH)
    .start()
)

logger.info("MedRoute Emergency Dispatch Streaming Engine is live and actively processing...")
dispatch_query.awaitTermination()