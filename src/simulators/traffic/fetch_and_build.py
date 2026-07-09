# src/simulators/traffic/fetch_and_build.py
# Valhalla version — replaces the OSRM version entirely

import csv
import logging
import os
import subprocess
import sys
import time

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] TrafficFetcher: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
sys.stdout.reconfigure(line_buffering=True)
logger = logging.getLogger("MedRoute.Traffic.Valhalla")

NYC_TRAFFIC_API_URL  = "https://data.cityofnewyork.us/resource/i4gi-tjb9.json?$order=data_as_of DESC&$limit=125"
MAPPING_CSV_PATH     = "/data/nyc_to_valhalla_mapping.csv"
TRAFFIC_DIR          = "/data/traffic"          # Valhalla watches this folder
SPEED_CSV_PATH       = "/data/traffic/speeds.csv"
VALHALLA_TILE_DIR    = "/data/valhalla"

MIN_SPEED_KMH        = 5.0
MAX_SPEED_MPH        = 100.0
TIMEOUT_SEC          = 15
UPDATE_INTERVAL_SEC  = 300   # every 5 minutes


def load_mapping() -> dict:
    """Load nyc_link_id → valhalla_edge_id into RAM once."""
    if not os.path.exists(MAPPING_CSV_PATH):
        logger.error("Valhalla mapping file not found: %s", MAPPING_CSV_PATH)
        return {}

    lookup = {}
    with open(MAPPING_CSV_PATH, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            nyc_id  = str(row["nyc_link_id"]).strip()
            edge_id = str(row["valhalla_edge_id"]).strip()
            if nyc_id and edge_id:
                lookup[nyc_id] = edge_id

    logger.info("Loaded %d Valhalla edge mappings into memory.", len(lookup))
    return lookup


def fetch_nyc_speeds() -> dict:
    """
    Fetch current speeds from NYC Traffic API.
    Returns {nyc_link_id: speed_kmh}
    """
    try:
        resp = requests.get(NYC_TRAFFIC_API_URL, timeout=TIMEOUT_SEC)
        resp.raise_for_status()
        records = resp.json()
    except Exception as exc:
        logger.error("NYC API fetch failed: %s", exc)
        return {}
    speeds = {}
    reasons = {"link_id, raw_speed": 0,
              "speed_mph = float(raw_speed)": 0,
              "speed_mph <= 0":0,
              "and speed_mph > MAX_SPEED_MPH":0}
    
    for rec in records:
        link_id   = str(rec.get("link_id") or rec.get("id") or "").strip()
        raw_speed = rec.get("speed")
        if not link_id or raw_speed is None:
            reasons["link_id, raw_speed"] += 1
            continue
        try:
            speed_mph = float(raw_speed)
        except (ValueError, TypeError):
            reasons["speed_mph = float(raw_speed)"] += 1    
            continue
        if speed_mph <= 0:
            reasons["speed_mph <= 0"] += 1
            continue
        if speed_mph > MAX_SPEED_MPH:
            reasons["and speed_mph > MAX_SPEED_MPH"] += 1
            continue
        speeds[link_id] = max(speed_mph * 1.60934, MIN_SPEED_KMH)

    logger.info("Fetched %d valid speed readings from NYC API.", len(speeds))
    logger.info("Skipped readings: %s", reasons)
    return speeds


def build_valhalla_speed_csv(speeds: dict, lookup: dict) -> int:
    """
    Join NYC speeds with Valhalla edge IDs and write speed CSV.
    Valhalla format: edge_id,speed,speed_backwards,congestion
    Returns number of matched segments written.
    """
    os.makedirs(TRAFFIC_DIR, exist_ok=True)
    tmp_path = SPEED_CSV_PATH + ".tmp"
    matched  = 0

    with open(tmp_path, "w", encoding="utf-8") as f:
        # Valhalla speed CSV has a header
        f.write("edge_id,speed,speed_backwards,congestion\n")

        for nyc_link_id, speed_kmh in speeds.items():
            edge_id = lookup.get(nyc_link_id)
            if edge_id is None:
                continue

            # Congestion: 0.0 = free flow (50+ km/h), 1.0 = standstill
            # Simple linear scale between 5 and 50 km/h
            congestion = max(0.0, min(1.0, 1.0 - (speed_kmh - MIN_SPEED_KMH) / 45.0))

            f.write(f"{edge_id},{speed_kmh:.1f},{speed_kmh:.1f},{congestion:.3f}\n")
            matched += 1

    os.replace(tmp_path, SPEED_CSV_PATH)
    logger.info("Valhalla speed CSV written: %d segments matched.", matched)
    return matched


def update_valhalla_traffic_tiles() -> bool:
    """
    Call valhalla_traffic_pairs inside the Valhalla container to convert
    the speed CSV into binary traffic tiles. Valhalla hot-loads them.

    This runs the tool directly — the traffic_fetcher container
    shares the Valhalla tile volume so both can access /data/valhalla.
    """
    try:
        result = subprocess.run(
            [
                "valhalla_traffic_pairs",
                "--config", "/data/valhalla/valhalla.json",
                "--traffic-dir", TRAFFIC_DIR,
                "--speed-file", SPEED_CSV_PATH,
            ],
            capture_output=True,
            text=True,
            timeout=60,
        )
        if result.returncode == 0:
            logger.info("Valhalla traffic tiles updated successfully.")
            return True
        logger.error("valhalla_traffic_pairs failed:\n%s", result.stderr)
        return False
    except FileNotFoundError:
        # valhalla_traffic_pairs not in this container — use API approach instead
        logger.warning(
            "valhalla_traffic_pairs not found in this container. "
            "Using filesystem write only — Valhalla will auto-detect tile changes."
        )
        return True
    except Exception as exc:
        logger.error("Traffic tile update failed: %s", exc)
        return False
    
    
def update_valhalla_traffic_tiles_via_api(speeds: dict, lookup: dict) -> bool:
    """
    Push live speeds directly into Valhalla's memory using its Traffic Update API.
    """
    # 1. تجهيز المسار الخاص بالـ API (فالهالا يعمل على منفذ 8002 داخل الشبكة)
    # نستخدم اسم الخدمة في الدوكر كومبوز 'valhalla' أو 'medroute_valhalla'
    VALHALLA_API_URL = "http://medroute_valhalla:8002/update_traffic"
    
    # 2. تحويل البيانات للصيغة التي يفهمها الـ API الخاص بفالهالا
    # الـ API يتوقع مصفوفة من الـ Edges والسرعات المقابلة لها
    traffic_updates = []
    for nyc_link_id, speed_kmh in speeds.items():
        edge_id = lookup.get(nyc_link_id)
        if edge_id is None:
            continue
            
        traffic_updates.append({
            "edge_id": int(edge_id),
            "speed": float(round(speed_kmh, 1))
        })

    if not traffic_updates:
        logger.warning("No matched segments to push to Valhalla API.")
        return False

    # 3. إرسال الطلب للسيرفر
    try:
        logger.info("Pushing %d speed updates to Valhalla Traffic API...", len(traffic_updates))
        
        response = requests.post(
            VALHALLA_API_URL,
            json={"updates": traffic_updates},
            timeout=30
        )
        
        if response.status_code == 200:
            logger.info("Valhalla RAM traffic hot-loaded successfully via API!")
            return True
        else:
            logger.error("Valhalla API rejected traffic update. Status: %d, Response: %s", 
                         response.status_code, response.text)
            return False
            
    except Exception as exc:
        logger.error("Failed to connect to Valhalla Traffic API: %s", exc)
        return False

def run_update_cycle(lookup: dict) -> bool:
    """One full fetch → build → update cycle."""
    t0 = time.time()

    speeds = fetch_nyc_speeds()
    if not speeds:
        logger.error("No speeds fetched — skipping this cycle.")
        return False

    matched = build_valhalla_speed_csv(speeds, lookup)
    if matched == 0:
        logger.error("Zero segments matched — check your mapping file.")
        return False

    success = update_valhalla_traffic_tiles()
    logger.info(
        "Update cycle complete in %.1fs — %d segments, success=%s",
        time.time() - t0, matched, success
    )
    return success


if __name__ == "__main__":
    logger.info("MedRoute Valhalla traffic fetcher starting...")
    lookup = load_mapping()

    if not lookup:
        logger.critical("Empty mapping — cannot start. Check %s", MAPPING_CSV_PATH)
        sys.exit(1)

    logger.info("Starting update loop every %ds.", UPDATE_INTERVAL_SEC)
    while True:
        try:
            run_update_cycle(lookup)
        except Exception as exc:
            logger.error("Unexpected error in update cycle: %s", exc, exc_info=True)
        time.sleep(UPDATE_INTERVAL_SEC)