package com.openlattice.collections.aggregators

import com.hazelcast.aggregation.Aggregator
import com.openlattice.edm.collection.CollectionTemplates
import com.openlattice.edm.collection.CollectionTemplateKey
import java.util.*

data class EntitySetCollectionConfigAggregator(
        val collectionTemplates: CollectionTemplates
) : Aggregator<Map.Entry<CollectionTemplateKey, UUID>, CollectionTemplates>() {

    override fun accumulate(input: Map.Entry<CollectionTemplateKey, UUID>) {
        val entitySetCollectionId = input.key.entitySetCollectionId
        val templateTypeId = input.key.templateTypeId
        val entitySetId = input.value

        collectionTemplates.put(entitySetCollectionId, templateTypeId, entitySetId)
    }

    override fun combine(aggregator: Aggregator<*, *>) {
        if (aggregator is EntitySetCollectionConfigAggregator) {
            collectionTemplates.putAll(aggregator.collectionTemplates.templates)
        }
    }

    override fun aggregate(): CollectionTemplates {
        return collectionTemplates
    }

}