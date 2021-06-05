package com.openlattice.hazelcast.serializers

import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest
import com.openlattice.collections.processors.UpdateEntitySetCollectionTemplateProcessor
import com.openlattice.mapstores.TestDataFactory

class UpdateEntitySetCollectionTemplateProcessorStreamSerializerTest
    : AbstractStreamSerializerTest<UpdateEntitySetCollectionTemplateProcessorStreamSerializer, UpdateEntitySetCollectionTemplateProcessor>() {

    override fun createSerializer(): UpdateEntitySetCollectionTemplateProcessorStreamSerializer {
        return UpdateEntitySetCollectionTemplateProcessorStreamSerializer()
    }

    override fun createInput(): UpdateEntitySetCollectionTemplateProcessor {
        return UpdateEntitySetCollectionTemplateProcessor(TestDataFactory.entitySetCollection().template)
    }
}