package com.openlattice.hazelcast.serializers

import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest
import com.openlattice.hazelcast.processors.RemoveEntitySetsFromLinkingEntitySetProcessor
import java.util.UUID

class RemoveEntitySetsFromLinkingEntitySetProcessorStreamSerializerTest:
        AbstractStreamSerializerTest<RemoveEntitySetsFromLinkingEntitySetProcessorStreamSerializer, RemoveEntitySetsFromLinkingEntitySetProcessor>() {

    override fun createSerializer(): RemoveEntitySetsFromLinkingEntitySetProcessorStreamSerializer {
        return RemoveEntitySetsFromLinkingEntitySetProcessorStreamSerializer()
    }

    override fun createInput(): RemoveEntitySetsFromLinkingEntitySetProcessor {
        return RemoveEntitySetsFromLinkingEntitySetProcessor( setOf( UUID.randomUUID() ) )
    }
}