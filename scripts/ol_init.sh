curl -XPUT http://localhost:9200/_cluster/settings -H 'Content-type: application/json' --data-binary $'{"transient":{"cluster.max_shards_per_node":10000}}'
./citus_init.sh

