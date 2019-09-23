package com.openlattice.hazelcast.serializers

import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest
import com.openlattice.collections.processors.UpdateEntityTypeCollectionMetadataProcessor
import com.openlattice.mapstores.TestDataFactory

class UpdateEntityTypeCollectionMetadataProcessorStreamSerializerTest:
        AbstractStreamSerializerTest<UpdateEntityTypeCollectionMetadataProcessorStreamSerializer, UpdateEntityTypeCollectionMetadataProcessor>() {
    override fun createSerializer(): UpdateEntityTypeCollectionMetadataProcessorStreamSerializer {
        return UpdateEntityTypeCollectionMetadataProcessorStreamSerializer()
    }

    override fun createInput(): UpdateEntityTypeCollectionMetadataProcessor {
        return UpdateEntityTypeCollectionMetadataProcessor(TestDataFactory.metadataUpdate())
    }
}