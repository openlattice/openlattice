#!/bin/bash
WIPE=$1
# Initialize elasticsearch by configuring max shards per node for elasticsearch
curl -XPUT http://localhost:9200/_cluster/settings -H 'Content-type: application/json' --data-binary $'{"transient":{"cluster.max_shards_per_node":10000}}'
curl -XPUT http://localhost:9200/_cluster/settings -H 'Content-type: application/json' --data-binary $'{"persistent":{"cluster.max_shards_per_node":10000}}'
# Initialize citus.
./citus_init.sh
if [ "$WIPE" = "wipe" ]
  then
  ./clear_elasticsearch_indexes.sh
fi
  
