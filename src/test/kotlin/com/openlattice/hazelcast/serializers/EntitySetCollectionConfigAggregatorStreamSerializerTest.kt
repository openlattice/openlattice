package com.openlattice.hazelcast.serializers

import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest
import com.openlattice.collections.aggregators.EntitySetCollectionConfigAggregator
import com.openlattice.mapstores.TestDataFactory

class EntitySetCollectionConfigAggregatorStreamSerializerTest : AbstractStreamSerializerTest<EntitySetCollectionConfigAggregatorStreamSerializer, EntitySetCollectionConfigAggregator>() {

    override fun createSerializer(): EntitySetCollectionConfigAggregatorStreamSerializer {
        return EntitySetCollectionConfigAggregatorStreamSerializer()
    }

    override fun createInput(): EntitySetCollectionConfigAggregator {
        return EntitySetCollectionConfigAggregator(TestDataFactory.collectionTemplates())
    }
}