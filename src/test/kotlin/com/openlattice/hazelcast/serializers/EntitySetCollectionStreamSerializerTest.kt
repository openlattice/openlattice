package com.openlattice.hazelcast.serializers

import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest
import com.openlattice.edm.collection.EntitySetCollection
import com.openlattice.mapstores.TestDataFactory

class EntitySetCollectionStreamSerializerTest: AbstractStreamSerializerTest<EntitySetCollectionStreamSerializer, EntitySetCollection>() {
    override fun createSerializer(): EntitySetCollectionStreamSerializer {
        return EntitySetCollectionStreamSerializer()
    }

    override fun createInput(): EntitySetCollection {
        return TestDataFactory.entitySetCollection()
    }
}