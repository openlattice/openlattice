package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer
import com.openlattice.collections.processors.UpdateEntitySetCollectionMetadataProcessor
import com.openlattice.hazelcast.StreamSerializerTypeIds
import org.springframework.stereotype.Component

@Component
class UpdateEntitySetCollectionMetadataProcessorStreamSerializer : SelfRegisteringStreamSerializer<UpdateEntitySetCollectionMetadataProcessor> {

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.UPDATE_ENTITY_SET_COLLECTION_METADATA_PROCESSOR.ordinal
    }

    override fun getClazz(): Class<out UpdateEntitySetCollectionMetadataProcessor> {
        return UpdateEntitySetCollectionMetadataProcessor::class.java
    }

    override fun write(out: ObjectDataOutput?, `object`: UpdateEntitySetCollectionMetadataProcessor) {
        MetadataUpdateStreamSerializer.serialize(out, `object`.update)
    }

    override fun read(`in`: ObjectDataInput?): UpdateEntitySetCollectionMetadataProcessor {
        return UpdateEntitySetCollectionMetadataProcessor(MetadataUpdateStreamSerializer.deserialize(`in`))
    }
}