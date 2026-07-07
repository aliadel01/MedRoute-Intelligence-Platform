import requests
import csv
import time

# Configurations - Using SODA2 public endpoint based on your exact dataset id
NYC_API_URL = "https://data.cityofnewyork.us/resource/i4gi-tjb9.json?$limit=150000"
OSRM_NEAREST_URL = "http://localhost:5000/nearest/v1/driving/"
OUTPUT_CSV_FILE = "nyc_to_osm_mapping.csv"

def get_osm_node(lon, lat):
    """Query OSRM for the nearest actual OSM Node ID"""
    url = f"{OSRM_NEAREST_URL}{lon},{lat}"
    try:
        response = requests.get(url, timeout=5)
        if response.status_code == 200:
            data = response.json()
            if "waypoints" in data and len(data["waypoints"]) > 0:
                return data["waypoints"][0]["nodes"][0]
    except Exception:
        pass
    return None

def main():
    print(f"1. Fetching live traffic links from NYC Open Data...")
    print(f"Target URL: {NYC_API_URL}")
    
    response = requests.get(NYC_API_URL)
    if response.status_code != 200:
        print(f"[-] Failed to fetch data. Status code: {response.status_code}")
        print(f"Response text: {response.text[:200]}")
        return
    
    traffic_records = response.json()
    print(f"[+] Successfully retrieved {len(traffic_records)} links.\n")

    if len(traffic_records) > 0:
        print("=== LIVE RECORD SAMPLE ===")
        import pprint
        pprint.pprint(traffic_records[0])
        print("==========================\n")
    else:
        print("[-] API returned an empty list!")
        return

    print("Starting OSRM mapping process...")

    with open(OUTPUT_CSV_FILE, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["nyc_link_id", "from_osm_node", "to_osm_node"])

        processed_count = 0
        
        for idx, link in enumerate(traffic_records):
            # Extract case-insensitive keys based on your table view
            link_id = link.get("link_id") or link.get("LINK_ID") or link.get("id")
            link_points = link.get("link_points") or link.get("LINK_POINTS")

            if not link_id or not link_points:
                continue

            try:
                # Parse string coordinates: "40.7024204,-73.816481 40.700841,-73.816500"
                points = [pt.strip() for pt in link_points.strip().split(" ") if pt.strip()]
                
                if len(points) < 2:
                    continue 
                
                start_coords = points[0].split(",") 
                end_coords = points[-1].split(",")   

                start_lat, start_lon = float(start_coords[0]), float(start_coords[1])
                end_lat, end_lon = float(end_coords[0]), float(end_coords[1])

                # Query OSRM using (longitude, latitude)
                from_node = get_osm_node(start_lon, start_lat)
                to_node = get_osm_node(end_lon, end_lat)

                if from_node and to_node:
                    writer.writerow([link_id, from_node, to_node])
                    processed_count += 1
                
            except Exception:
                continue

            # Progress tracking output
            if processed_count > 0 and processed_count % 50 == 0:
                print(f"[+] Mapped {processed_count} links successfully...")
                time.sleep(0.01)

    print(f"\n Done! Mapping file saved to: {OUTPUT_CSV_FILE}")
    print(f"Total successfully mapped links: {processed_count}")

if __name__ == "__main__":
    main()