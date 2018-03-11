Conductor
=======================

In-Memory Data Grid and Execution Engine

Conductor provides and in-memory cache and other distributed data structures for processing data requests.

## Setup
1. Install elasticsearch and its analysis-phonetic plugin
```
brew install elasticsearch
elasticsearch-plugin install analysis-phonetic
```
2. Run elasticsearch with cluster name "openlattice"
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
curl -XDELETE 'http://localhost:9200/securable_object_*'
```
