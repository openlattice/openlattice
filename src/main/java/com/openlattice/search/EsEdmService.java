package com.openlattice.search;

import com.openlattice.conductor.rpc.ConductorElasticsearchApi;
import com.openlattice.edm.type.PropertyType;


public class EsEdmService {

    private ConductorElasticsearchApi elasticsearchApi;

    public EsEdmService(ConductorElasticsearchApi elasticsearchApi) {
        this.elasticsearchApi = elasticsearchApi;
    }

    public void createPropertyType( PropertyType propertyType ) {
        elasticsearchApi.savePropertyTypeToElasticsearch( propertyType );
    }
}
