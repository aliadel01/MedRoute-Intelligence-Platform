# run in simulator/routing/data folder
# osrm-extract
docker run -t -v "${PWD}:/data" osrm/osrm-backend osrm-extract -p /opt/car.lua /data/nyc-roads.osm.pbf

# osrm-partition
docker run -t -v "${PWD}:/data" osrm/osrm-backend osrm-partition /data/nyc-roads.osm.pbf

# osrm-customize
docker run -t -v "${PWD}:/data" osrm/osrm-backend osrm-customize /data/nyc-roads.osm.pbf

# Routing Server
docker run -d --name osrm-medroute -p 5000:5000 -v "${PWD}:/data" osrm/osrm-backend osrm-routed --algorithm mld /data/nyc-roads.osm.pbf
# http://localhost:5000/route/v1/driving/-73.985130,40.758896;-73.991234,40.748432?overview=full&geometries=geojson