package com.openlattice.hazelcast.serializers

import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest
import com.openlattice.hazelcast.processors.AddEntitySetsToLinkingEntitySetProcessor
import java.util.UUID

class AddEntitySetsToLinkingEntitySetProcessorStreamSerializerTest:
        AbstractStreamSerializerTest<AddEntitySetsToLinkingEntitySetProcessorStreamSerializer, AddEntitySetsToLinkingEntitySetProcessor>() {

    override fun createSerializer(): AddEntitySetsToLinkingEntitySetProcessorStreamSerializer {
        return AddEntitySetsToLinkingEntitySetProcessorStreamSerializer()
    }

    override fun createInput(): AddEntitySetsToLinkingEntitySetProcessor {
        return AddEntitySetsToLinkingEntitySetProcessor( setOf( UUID.randomUUID() ) )
    }
}