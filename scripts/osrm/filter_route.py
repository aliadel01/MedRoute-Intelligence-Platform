import osmium

class RoadFilterHandler(osmium.SimpleHandler):
    def __init__(self, writer):
        super(RoadFilterHandler, self).__init__()
        self.writer = writer
        # Define the allowed highway types for cars
        self.valid_highways = {
            'motorway', 'trunk', 'primary', 'secondary', 'tertiary', 'residential',
            'motorway_link', 'trunk_link', 'primary_link', 'secondary_link', 
            'tertiary_link', 'unclassified'
        }
        # Define types to explicitly reject
        self.rejected_highways = {'footway', 'cycleway', 'path', 'steps', 'pedestrian'}

    def way(self, w):
        # Check if the way has a 'highway' tag
        if 'highway' in w.tags:
            highway_type = w.tags['highway']
            
            # Accept only car roads and strictly reject pedestrian/cycle paths
            if highway_type in self.valid_highways and highway_type not in self.rejected_highways:
                # Write the valid way to the output file
                self.writer.add_way(w)

    def relation(self, r):
        # Keep turn restrictions for the routing engine
        if 'type' in r.tags and r.tags['type'] == 'restriction':
            self.writer.add_relation(r)

    def node(self, n):
        # In osmium, to apply '--used-node' automatically during writing,
        # we can pass the nodes through, and the writer can handle the referencing.
        # However, to be safe and optimize, we can write nodes, and osmium's 
        # file format writer manages the topology.
        self.writer.add_node(n)

def main():
    # Define file paths
    input_file = "./simulator/routing/data/new-york-260627.osm.pbf"
    output_file = "./simulator/routing/data/nyc-roads.osm.pbf"

    # Initialize the Osmium writer for the output file
    writer = osmium.SimpleWriter(output_file)
    
    # Initialize our custom handler with the writer
    handler = RoadFilterHandler(writer)
    
    print("Starting python-based road filtration... This might take a minute.")
    # Apply the handler to scan and filter the input file
    handler.apply_file(input_file, locations=True) # locations=True ensures node coordinates are cached
    
    # Close the writer to flush data to disk
    writer.close()
    print(f"Filtration complete! Cleaned file saved to: {output_file}")

if __name__ == '__main__':
    main()