# build_valhalla_mapping.py
# Run this ONCE after Valhalla is set up to rebuild your mapping

import os

import requests
import csv
import time
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger("MedRoute.ValhallaMappingBuilder")

NYC_API_URL = "https://data.cityofnewyork.us/resource/i4gi-tjb9.json?$order=data_as_of DESC&$limit=125"
VALHALLA_URL      = "http://localhost:8002"
OUTPUT_CSV        = "data/valhalla/nyc_to_valhalla_mapping.csv"


def get_valhalla_edge_id(lon: float, lat: float) -> int | None:
    """
    Query Valhalla /locate to get the nearest edge ID.
    Returns the edge_id integer or None if not found.
    """
    payload = {
        "locations": [{"lon": lon, "lat": lat}],
        "costing": "auto",
        "directions_options": {"units": "kilometers"}
    }
    try:
        resp = requests.post(
            f"{VALHALLA_URL}/locate",
            json=payload,
            timeout=5
        )
        if resp.status_code == 200:
            data = resp.json()
            edges = data[0].get("edges", [])
            if edges:
                # Return the edge ID of the nearest road edge
                nearest_edge = edges[0]
                if "way_id" in nearest_edge:
                    return nearest_edge["way_id"]
                elif "edge_id" in nearest_edge:
                    return nearest_edge["edge_id"]
    except Exception as exc:
        logger.debug("Locate failed for %.6f,%.6f: %s", lat, lon, exc)
    return None


def build_mapping():
    logger.info("Fetching NYC traffic links...")
    resp = requests.get(NYC_API_URL, timeout=15)
    resp.raise_for_status()
    records = resp.json()
    logger.info("Fetched %d links. Building Valhalla edge mapping...", len(records))

    os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["nyc_link_id", "valhalla_edge_id"])

        mapped = 0
        for idx, link in enumerate(records):
            link_id     = link.get("link_id")
            link_points = link.get("link_points", "")

            if not link_id or not link_points:
                continue

            points = [p.strip() for p in link_points.strip().split() if p.strip()]
            if len(points) < 1:
                continue

            try:
                mid_idx = len(points) // 2
                lat_str, lon_str = points[mid_idx].split(",")
                lat = float(lat_str.strip())
                lon = float(lon_str.strip())
            except (ValueError, IndexError):
                continue

            if idx < 3:
                logger.info(f"Testing Link {link_id}: Midpoint is lat={lat}, lon={lon}")

            edge_id = get_valhalla_edge_id(lon, lat)
            
            if idx < 3:
                logger.info(f"Valhalla response for Link {link_id}: edge_id={edge_id}")

            if edge_id is not None:
                writer.writerow([link_id, edge_id])
                mapped += 1
                
            # Rate limit — Valhalla is local so this can be fast
            time.sleep(0.005)

    logger.info("Done. Mapped %d links → %s", mapped, OUTPUT_CSV)


if __name__ == "__main__":
    build_mapping()