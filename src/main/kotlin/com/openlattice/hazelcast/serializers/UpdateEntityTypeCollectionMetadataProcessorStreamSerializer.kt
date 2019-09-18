package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer
import com.openlattice.collections.processors.UpdateEntityTypeCollectionMetadataProcessor
import com.openlattice.hazelcast.StreamSerializerTypeIds
import org.springframework.stereotype.Component

@Component
class UpdateEntityTypeCollectionMetadataProcessorStreamSerializer : SelfRegisteringStreamSerializer<UpdateEntityTypeCollectionMetadataProcessor> {
    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.UPDATE_ENTITY_TYPE_COLLECTION_METADATA_PROCESSOR.ordinal
    }

    override fun getClazz(): Class<out UpdateEntityTypeCollectionMetadataProcessor> {
        return UpdateEntityTypeCollectionMetadataProcessor::class.java
    }

    override fun write(out: ObjectDataOutput?, `object`: UpdateEntityTypeCollectionMetadataProcessor) {
        MetadataUpdateStreamSerializer.serialize(out, `object`.update)
    }

    override fun read(`in`: ObjectDataInput?): UpdateEntityTypeCollectionMetadataProcessor {
        val update = MetadataUpdateStreamSerializer.deserialize(`in`)
        return UpdateEntityTypeCollectionMetadataProcessor(update)
    }
}