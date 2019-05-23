package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer
import com.openlattice.collections.processors.AddPairToEntityTypeCollectionTemplateProcessor
import com.openlattice.hazelcast.StreamSerializerTypeIds
import org.springframework.stereotype.Component

@Component
class AddPairToEntityTypeCollectionTemplateProcessorStreamSerializer : SelfRegisteringStreamSerializer<AddPairToEntityTypeCollectionTemplateProcessor> {
    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.ADD_PAIR_TO_ENTITY_TYPE_COLLECTION_TEMPLATE_PROCESSOR.ordinal
    }

    override fun getClazz(): Class<out AddPairToEntityTypeCollectionTemplateProcessor> {
        return AddPairToEntityTypeCollectionTemplateProcessor::class.java
    }

    override fun write(out: ObjectDataOutput, `object`: AddPairToEntityTypeCollectionTemplateProcessor) {
        CollectionTemplateTypeStreamSerializer.serialize(out, `object`.collectionTemplateType)
    }

    override fun read(`in`: ObjectDataInput): AddPairToEntityTypeCollectionTemplateProcessor {
        return AddPairToEntityTypeCollectionTemplateProcessor(CollectionTemplateTypeStreamSerializer.deserialize(`in`))
    }
}