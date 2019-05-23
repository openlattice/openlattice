package com.openlattice.hazelcast.serializers

import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest
import com.openlattice.edm.collection.CollectionTemplates
import com.openlattice.mapstores.TestDataFactory

class CollectionTemplatesStreamSerializerTest : AbstractStreamSerializerTest<CollectionTemplatesStreamSerializer, CollectionTemplates>() {

    override fun createSerializer(): CollectionTemplatesStreamSerializer {
        return CollectionTemplatesStreamSerializer()
    }

    override fun createInput(): CollectionTemplates {
        return TestDataFactory.collectionTemplates()
    }
}