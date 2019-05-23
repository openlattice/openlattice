package com.openlattice.hazelcast.serializers

import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest
import com.openlattice.edm.collection.EntityTypeCollection
import com.openlattice.mapstores.TestDataFactory

class EntityTypeCollectionStreamSerializerTest: AbstractStreamSerializerTest<EntityTypeCollectionStreamSerializer, EntityTypeCollection>() {
    override fun createSerializer(): EntityTypeCollectionStreamSerializer {
        return EntityTypeCollectionStreamSerializer()
    }

    override fun createInput(): EntityTypeCollection {
        return TestDataFactory.entityTypeCollection()
    }
}