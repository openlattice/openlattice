package com.openlattice.hazelcast.serializers

import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest
import com.openlattice.edm.collection.CollectionTemplateKey
import java.util.*

class CollectionTemplateKeyStreamSerializerTest : AbstractStreamSerializerTest<CollectionTemplateKeyStreamSerializer, CollectionTemplateKey>() {

    override fun createSerializer(): CollectionTemplateKeyStreamSerializer {
        return CollectionTemplateKeyStreamSerializer()
    }

    override fun createInput(): CollectionTemplateKey {
        return CollectionTemplateKey(UUID.randomUUID(), UUID.randomUUID())
    }
}