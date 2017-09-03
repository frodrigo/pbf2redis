# pbf2redis
Experiment on switching from SQL to Redis to request OpenStreetMap topological data.

This the source code of this article: https://medium.com/@frederic.rodrigo/pbf2redis-eae7fcada735

# Recipie

Intall ruby, then install gem:
```
gem install redis redic
```

Install Redis and Tile38 (optional).

Download a, small, .osm.pbf file from http://download.geofabrik.de/

Load OpenStreetMap data into Redis and Tile38
```
ruby pbf2redis.rb ile-de-france-latest.osm.pbf
```

Run test queries
```
time ruby query1.rb
time ruby query2.0.rb
time ruby query2.1.rb
time ruby query2.2.rb
time ruby query2-spatial.rb
```
