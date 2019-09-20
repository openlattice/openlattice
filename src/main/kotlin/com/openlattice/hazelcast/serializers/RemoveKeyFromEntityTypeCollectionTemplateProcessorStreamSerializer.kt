package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer
import com.openlattice.collections.processors.RemoveKeyFromEntityTypeCollectionTemplateProcessor
import com.openlattice.hazelcast.StreamSerializerTypeIds
import org.springframework.stereotype.Component

@Component
class RemoveKeyFromEntityTypeCollectionTemplateProcessorStreamSerializer : SelfRegisteringStreamSerializer<RemoveKeyFromEntityTypeCollectionTemplateProcessor> {

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.REMOVE_KEY_FROM_ENTITY_TYPE_COLLECTION_TEMPLATE_PROCESSOR.ordinal
    }

    override fun getClazz(): Class<out RemoveKeyFromEntityTypeCollectionTemplateProcessor> {
        return RemoveKeyFromEntityTypeCollectionTemplateProcessor::class.java
    }

    override fun write(out: ObjectDataOutput, `object`: RemoveKeyFromEntityTypeCollectionTemplateProcessor) {
        UUIDStreamSerializer.serialize(out, `object`.templateTypeId)
    }

    override fun read(`in`: ObjectDataInput): RemoveKeyFromEntityTypeCollectionTemplateProcessor {
        return RemoveKeyFromEntityTypeCollectionTemplateProcessor(UUIDStreamSerializer.deserialize(`in`))
    }
}