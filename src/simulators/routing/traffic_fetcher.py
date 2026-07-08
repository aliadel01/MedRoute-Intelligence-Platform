# routing/fetch_and_build.py
# Combines your RAM-cached mapping approach with the guide's
# validation, speed floor, and unit conversion

import csv
import logging
import os
import sys
import time
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] TrafficFetcher: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("MedRoute.Traffic")

NYC_TRAFFIC_API_URL = "https://data.cityofnewyork.us/resource/i4gi-tjb9.json?$limit=150000"
TRAFFIC_CSV_PATH    = "/data/traffic.csv"
MAPPING_CSV_PATH    = "/data/nyc_to_osm_mapping.csv"
MIN_SPEED_KMH       = 5.0    # floor — prevents impassable roads from bad readings
MAX_SPEED_MPH       = 100.0  # ceiling — filters sensor errors
TIMEOUT_SEC         = 15


def load_mapping() -> dict:
    """Load mapping CSV into RAM once at startup."""
    if not os.path.exists(MAPPING_CSV_PATH):
        logger.error("Mapping file not found: %s", MAPPING_CSV_PATH)
        return {}

    lookup = {}
    with open(MAPPING_CSV_PATH, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            nyc_id = str(row["nyc_link_id"]).strip()
            from_n = row["from_osm_node"].strip()
            to_n   = row["to_osm_node"].strip()
            if nyc_id and from_n and to_n:
                lookup[nyc_id] = (from_n, to_n)

    logger.info("Loaded %d link mappings into memory.", len(lookup))
    return lookup


def fetch_and_build(lookup: dict) -> bool:
    """
    Fetch NYC speeds, join with mapping, write OSRM segment speed CSV.
    Returns True on success, False on failure.
    Writes atomically — OSRM never sees a partial file.
    """
    if not lookup:
        logger.error("Empty mapping — cannot build speed CSV.")
        return False

    t0 = time.time()
    try:
        resp = requests.get(NYC_TRAFFIC_API_URL, timeout=TIMEOUT_SEC)
        resp.raise_for_status()
        records = resp.json()
    except Exception as exc:
        logger.error("NYC API fetch failed: %s", exc)
        return False

    logger.info("Fetched %d records in %.1fs.", len(records), time.time() - t0)

    matched   = 0
    skipped   = 0
    tmp_path  = TRAFFIC_CSV_PATH + ".tmp"
    skipped_reasons = {
        "missing_link_id": 0,
        "invalid_speed": 0,
        "out_of_bounds_speed": 0,
        "missing_mapping": 0,
    }
    with open(tmp_path, "w", encoding="utf-8") as f:
        # NO HEADER — osrm-customize requires headerless CSV
        for rec in records:
            link_id   = str(rec.get("link_id") or rec.get("id") or "").strip()
            raw_speed = rec.get("speed")

            if not link_id or raw_speed is None:
                skipped += 1
                skipped_reasons["missing_link_id"] += 1
                continue

            try:
                speed_mph = float(raw_speed)
            except (ValueError, TypeError):
                skipped += 1
                skipped_reasons["invalid_speed"] += 1
                continue

            if speed_mph <= 0 or speed_mph > MAX_SPEED_MPH:
                skipped += 1
                skipped_reasons["out_of_bounds_speed"] += 1
                continue

            nodes = lookup.get(link_id)
            if nodes is None:
                skipped += 1
                skipped_reasons["missing_mapping"] += 1
                continue

            from_node, to_node = nodes
            speed_kmh = max(speed_mph * 1.60934, MIN_SPEED_KMH)
            f.write(f"{int(from_node)},{int(to_node)},{speed_kmh:.2f}\n")
            matched += 1

    # Atomic replace — only happens if write succeeded
    os.replace(tmp_path, TRAFFIC_CSV_PATH)

    logger.info(
        "Speed CSV written in %.1fs — %d matched, %d skipped.",
        time.time() - t0, matched, skipped,
    )
    
    logger.info(f"Skipped reasons: {skipped_reasons}")
    
    return matched > 0


if __name__ == "__main__":
    mapping = load_mapping()
    logger.info("Traffic fetcher service started globally...")
    
    while True:
        try:
            success = fetch_and_build(mapping)
            logger.info("Cycle finished. Success: %s", success)
        except Exception as e:
            logger.error("Unexpected error in loop: %s", e)
            
        time.sleep(300)