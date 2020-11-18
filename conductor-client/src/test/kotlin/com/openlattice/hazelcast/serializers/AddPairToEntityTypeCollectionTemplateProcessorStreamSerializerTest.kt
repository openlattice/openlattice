package com.openlattice.hazelcast.serializers

import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest
import com.openlattice.collections.processors.AddPairToEntityTypeCollectionTemplateProcessor
import com.openlattice.mapstores.TestDataFactory

class AddPairToEntityTypeCollectionTemplateProcessorStreamSerializerTest
    : AbstractStreamSerializerTest<AddPairToEntityTypeCollectionTemplateProcessorStreamSerializer, AddPairToEntityTypeCollectionTemplateProcessor>() {

    override fun createSerializer(): AddPairToEntityTypeCollectionTemplateProcessorStreamSerializer {
        return AddPairToEntityTypeCollectionTemplateProcessorStreamSerializer()
    }

    override fun createInput(): AddPairToEntityTypeCollectionTemplateProcessor {
        return AddPairToEntityTypeCollectionTemplateProcessor(TestDataFactory.collectionTemplateType())
    }

}