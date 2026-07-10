from __future__ import annotations

import logging
import os
import sys

import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import coalesce, col, from_unixtime, get_json_object, to_timestamp

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "redpanda-0:29092")
KAFKA_HOSPITALS_CDC_TOPIC = os.getenv("KAFKA_HOSPITALS_CDC_TOPIC", "cdc.public.hospitals")
CHECKPOINT_PATH = os.getenv("HOSPITALS_OLAP_CHECKPOINT_PATH", "/opt/spark/checkpoints/hospitals_olap_cdc")

POSTGRES_CONFIG = {
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "medroute_pass"),
    "host": os.getenv("POSTGRES_HOST", "medroute_postgres"),
    "port": os.getenv("POSTGRES_PORT", "5432"),
    "database": os.getenv("POSTGRES_DB", "medroute_db"),
}

HOSPITALS_OLAP_TABLE = os.getenv("HOSPITALS_OLAP_TABLE", "hospitals_olap")

CREATE_HOSPITALS_OLAP_SQL = f"""
CREATE TABLE IF NOT EXISTS {HOSPITALS_OLAP_TABLE} (
    hospital_id TEXT PRIMARY KEY,
    name TEXT,
    status TEXT,
    beds INTEGER,
    icu_beds INTEGER,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    source_cdc_op CHAR(1) NOT NULL,
    source_ts TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"""

UPSERT_SQL = f"""
INSERT INTO {HOSPITALS_OLAP_TABLE} (
    hospital_id,
    name,
    status,
    beds,
    icu_beds,
    latitude,
    longitude,
    source_cdc_op,
    source_ts
)
VALUES %s
ON CONFLICT (hospital_id) DO UPDATE SET
    name = EXCLUDED.name,
    status = EXCLUDED.status,
    beds = EXCLUDED.beds,
    icu_beds = EXCLUDED.icu_beds,
    latitude = EXCLUDED.latitude,
    longitude = EXCLUDED.longitude,
    source_cdc_op = EXCLUDED.source_cdc_op,
    source_ts = EXCLUDED.source_ts,
    updated_at = CURRENT_TIMESTAMP;
"""

logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("MedRoute.HospitalsCdcToOlap")


def _get_connection():
    return psycopg2.connect(
        dbname=POSTGRES_CONFIG["database"],
        user=POSTGRES_CONFIG["user"],
        password=POSTGRES_CONFIG["password"],
        host=POSTGRES_CONFIG["host"],
        port=POSTGRES_CONFIG["port"],
    )


def _write_batch_to_postgres(batch_df: DataFrame, batch_id: int) -> None:
    if batch_df.isEmpty():
        logger.info("Batch %d is empty; skipping.", batch_id)
        return

    rows = batch_df.select(
        "hospital_id",
        "name",
        "status",
        "beds",
        "icu_beds",
        "latitude",
        "longitude",
        "op",
        "source_ts",
    ).collect()

    upserts = []
    deletes = []
    for row in rows:
        hospital_id = row["hospital_id"]
        op = row["op"]
        if op == "d":
            deletes.append(hospital_id)
            continue

        upserts.append(
            (
                hospital_id,
                row["name"],
                row["status"],
                row["beds"],
                row["icu_beds"],
                row["latitude"],
                row["longitude"],
                op,
                row["source_ts"],
            )
        )

    with _get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_HOSPITALS_OLAP_SQL)

            if upserts:
                execute_values(cur, UPSERT_SQL, upserts, page_size=1000)

            if deletes:
                cur.execute(f"DELETE FROM {HOSPITALS_OLAP_TABLE} WHERE hospital_id = ANY(%s)", (deletes,))

    logger.info("Batch %d applied: upserts=%d, deletes=%d", batch_id, len(upserts), len(deletes))


def main() -> None:
    logger.info(
        "Starting hospitals CDC sync: topic=%s, checkpoint=%s",
        KAFKA_HOSPITALS_CDC_TOPIC,
        CHECKPOINT_PATH,
    )

    spark = (
        SparkSession.builder.appName("MedRoute.HospitalsCdcToOlap")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
        .config("spark.driver.extraClassPath", "/root/.ivy2/jars/*")
        .config("spark.executor.extraClassPath", "/root/.ivy2/jars/*")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_HOSPITALS_CDC_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed_stream = (
        raw_stream.select(
            col("key").cast("string").alias("key_str"),
            col("value").cast("string").alias("value_str"),
        )
        .filter(col("value_str").isNotNull())
        .select(
            coalesce(
                get_json_object(col("value_str"), "$.payload.after.id"),
                get_json_object(col("value_str"), "$.payload.before.id"),
                get_json_object(col("key_str"), "$.payload.id"),  # or just "$.id" depending on how Kafka key is structured
            ).alias("hospital_id"),
            get_json_object(col("value_str"), "$.payload.op").alias("op"),
            get_json_object(col("value_str"), "$.payload.after.name").alias("name"),
            get_json_object(col("value_str"), "$.payload.after.status").alias("status"),
            get_json_object(col("value_str"), "$.payload.after.beds").cast("int").alias("beds"),
            get_json_object(col("value_str"), "$.payload.after.icu_beds").cast("int").alias("icu_beds"),
            get_json_object(col("value_str"), "$.payload.after.latitude").cast("double").alias("latitude"),
            get_json_object(col("value_str"), "$.payload.after.longitude").cast("double").alias("longitude"),
            to_timestamp(
                from_unixtime(get_json_object(col("value_str"), "$.payload.source.ts_ms").cast("double") / 1000.0)
            ).alias("source_ts"),
        )
        .filter(col("hospital_id").isNotNull())
        .filter(col("op").isin("r", "c", "u", "d"))
    )

    query = (
        parsed_stream.writeStream.foreachBatch(_write_batch_to_postgres)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .start()
    )

    logger.info("Hospitals CDC-to-OLAP stream is running.")
    query.awaitTermination()


if __name__ == "__main__":
    main()
