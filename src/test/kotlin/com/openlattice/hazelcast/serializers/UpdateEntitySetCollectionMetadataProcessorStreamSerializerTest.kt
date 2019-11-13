package com.openlattice.hazelcast.serializers

import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest
import com.openlattice.collections.processors.UpdateEntitySetCollectionMetadataProcessor
import com.openlattice.mapstores.TestDataFactory

class UpdateEntitySetCollectionMetadataProcessorStreamSerializerTest :
        AbstractStreamSerializerTest<UpdateEntitySetCollectionMetadataProcessorStreamSerializer, UpdateEntitySetCollectionMetadataProcessor>() {
    override fun createSerializer(): UpdateEntitySetCollectionMetadataProcessorStreamSerializer {
        return UpdateEntitySetCollectionMetadataProcessorStreamSerializer()
    }

    override fun createInput(): UpdateEntitySetCollectionMetadataProcessor {
        return UpdateEntitySetCollectionMetadataProcessor(TestDataFactory.metadataUpdate())
    }
}