package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer
import com.openlattice.collections.processors.UpdateEntitySetCollectionTemplateProcessor
import com.openlattice.hazelcast.StreamSerializerTypeIds
import org.springframework.stereotype.Component
import java.util.*

@Component
class UpdateEntitySetCollectionTemplateProcessorStreamSerializer : SelfRegisteringStreamSerializer<UpdateEntitySetCollectionTemplateProcessor> {
    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.UPDATE_ENTITY_SET_COLLECTION_TEMPLATE_PROCESSOR.ordinal
    }

    override fun getClazz(): Class<out UpdateEntitySetCollectionTemplateProcessor> {
        return UpdateEntitySetCollectionTemplateProcessor::class.java
    }

    override fun write(out: ObjectDataOutput, `object`: UpdateEntitySetCollectionTemplateProcessor) {
        val template = `object`.template
        out.writeInt(template.size)
        template.entries.forEach {
            UUIDStreamSerializer.serialize(out, it.key)
            UUIDStreamSerializer.serialize(out, it.value)
        }
    }

    override fun read(`in`: ObjectDataInput): UpdateEntitySetCollectionTemplateProcessor {
        val size = `in`.readInt()

        val template = mutableMapOf<UUID, UUID>()

        for (i in 0 until size) {
            val id = UUIDStreamSerializer.deserialize(`in`)
            val entitySetId = UUIDStreamSerializer.deserialize(`in`)

            template[id] = entitySetId
        }

        return UpdateEntitySetCollectionTemplateProcessor(template)
    }
}