import osmium
import json

class GeoJSONExporter(osmium.SimpleHandler):
    def __init__(self):
        super(GeoJSONExporter, self).__init__()
        self.features = []

    def way(self, w):
        # We only care about ways with coordinates (LineStrings)
        # osmium extracts node locations if locations=True is passed to apply_file
        try:
            coordinates = [[n.lon, n.lat] for n in w.nodes]
            
            # Create standard GeoJSON Feature for each road segment
            feature = {
                "type": "Feature",
                "geometry": {
                    "type": "LineString",
                    "coordinates": coordinates
                },
                "properties": {
                    "@id": f"way/{w.id}",
                    "highway": w.tags.get('highway', ''),
                    "name": w.tags.get('name', 'Unknown Road'),
                    "maxspeed": w.tags.get('maxspeed', ''),
                    "oneway": w.tags.get('oneway', 'no'),
                    "lanes": w.tags.get('lanes', '')
                }
            }
            self.features.append(feature)
        except osmium.InvalidLocationError:
            # Skip ways that have incomplete node location data
            pass

def main():
    input_pbf = "./simulator/routing/data/nyc-roads.osm.pbf"
    output_geojson = "./simulator/routing/data/nyc-roads.geojson"

    exporter = GeoJSONExporter()
    
    print("Exporting PBF to GeoJSON format... Please wait.")
    # locations=True is mandatory here to extract actual coordinates for the lines
    exporter.apply_file(input_pbf, locations=True)

    # Wrap features into a standard GeoJSON FeatureCollection
    geojson_data = {
        "type": "FeatureCollection",
        "features": exporter.features
    }

    # Write the result to a text file
    with open(output_geojson, 'w', encoding='utf-8') as f:
        json.dump(geojson_data, f, ensure_ascii=False, indent=2)

    print(f"Success! GeoJSON file created with {len(exporter.features)} roads.")
    print(f"Saved to: {output_geojson}")

if __name__ == '__main__':
    main()