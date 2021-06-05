package com.openlattice.hazelcast.serializers

import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest
import com.openlattice.collections.processors.RemoveKeyFromEntityTypeCollectionTemplateProcessor
import java.util.*

class RemoveKeyFromEntityTypeCollectionTemplateProcessorStreamSerializerTest
    : AbstractStreamSerializerTest<RemoveKeyFromEntityTypeCollectionTemplateProcessorStreamSerializer, RemoveKeyFromEntityTypeCollectionTemplateProcessor>() {

    override fun createSerializer(): RemoveKeyFromEntityTypeCollectionTemplateProcessorStreamSerializer {
        return RemoveKeyFromEntityTypeCollectionTemplateProcessorStreamSerializer()
    }

    override fun createInput(): RemoveKeyFromEntityTypeCollectionTemplateProcessor {
        return RemoveKeyFromEntityTypeCollectionTemplateProcessor(UUID.randomUUID())
    }


}