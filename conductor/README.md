Conductor
=======================

Conductor is the cluster manager for the OpenLattice platform. It handles dispatching search queries, managing the entity data model, synchronizing roles with auth0, and materializing views.

It is built on top of Hazelcast a grid computing technology that provides distributed data structures and locking useful for safely distributing work like indexing, linking, and materializing.

## Setup
1. Ensure Postgres 10 or later is installed and listening on localhost port 5432. Also make sure that a super user oltest with password test exists.
2. Install elasticsearch and its analysis-phonetic plugin
```
brew install elasticsearch
elasticsearch-plugin install analysis-phonetic
```
3. Run elasticsearch with cluster name "openlattice"
```
elasticsearch -E cluster.name=openlattice
```

## Cleanup
Remove all elasticsearch indices and the data they contain
```
curl -XDELETE 'http://localhost:9200/entity_set_data_model'
curl -XDELETE 'http://localhost:9200/organizations'
curl -XDELETE 'http://localhost:9200/property_type_index'
curl -XDELETE 'http://localhost:9200/entity_type_index'
curl -XDELETE 'http://localhost:9200/association_type_index'
curl -XDELETE 'http://localhost:9200/app_index'
curl -XDELETE 'http://localhost:9200/app_type_index'
curl -XDELETE 'http://localhost:9200/entity_data_*'
```
