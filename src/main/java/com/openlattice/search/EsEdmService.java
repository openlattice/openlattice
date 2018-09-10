package com.openlattice.search;

import com.codahale.metrics.annotation.Timed;
import com.openlattice.conductor.rpc.ConductorElasticsearchApi;
import com.openlattice.edm.EntitySet;
import com.openlattice.edm.type.PropertyType;

import javax.inject.Inject;
import java.util.List;

public class EsEdmService {

    @Inject
    private ConductorElasticsearchApi elasticsearchApi;

    @Timed
    public void createEntitySet(EntitySet entitySet, List<PropertyType> propertyTypes ) {
        elasticsearchApi.saveEntitySetToElasticsearch( entitySet, propertyTypes );
    }

    public void createPropertyType( PropertyType propertyType ) {
        elasticsearchApi.savePropertyTypeToElasticsearch( propertyType );
    }
}
