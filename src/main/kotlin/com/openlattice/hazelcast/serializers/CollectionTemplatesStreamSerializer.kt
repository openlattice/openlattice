package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer
import com.openlattice.edm.collection.CollectionTemplates
import com.openlattice.hazelcast.StreamSerializerTypeIds
import org.springframework.stereotype.Component
import java.util.*

@Component
class CollectionTemplatesStreamSerializer : SelfRegisteringStreamSerializer<CollectionTemplates> {

    companion object {

        fun serialize(out: ObjectDataOutput, `object`: CollectionTemplates) {
            out.writeInt(`object`.templates.size)

            `object`.templates.forEach {
                UUIDStreamSerializer.serialize(out, it.key)
                out.writeInt(it.value.size)

                it.value.forEach  {
                    UUIDStreamSerializer.serialize(out, it.key)
                    UUIDStreamSerializer.serialize(out, it.value)
                }
            }
        }

        fun deserialize(`in`: ObjectDataInput): CollectionTemplates {
            val size = `in`.readInt()

            val templates = mutableMapOf<UUID, Map<UUID, UUID>>()

            for (i in 0 until size) {

                val entitySetCollectionId = UUIDStreamSerializer.deserialize(`in`)
                val templateSize = `in`.readInt()

                val templateMap = mutableMapOf<UUID, UUID>()

                for (j in 0 until templateSize) {
                    val templateTypeId = UUIDStreamSerializer.deserialize(`in`)
                    val entitySetId = UUIDStreamSerializer.deserialize(`in`)

                    templateMap[templateTypeId] = entitySetId
                }

                templates[entitySetCollectionId] = templateMap
            }

            return CollectionTemplates(templates)
        }
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.COLLECTION_TEMPLATES.ordinal
    }

    override fun getClazz(): Class<out CollectionTemplates> {
        return CollectionTemplates::class.java
    }

    override fun write(out: ObjectDataOutput, `object`: CollectionTemplates) {
        serialize(out, `object`)
    }

    override fun read(`in`: ObjectDataInput): CollectionTemplates {
        return deserialize(`in`)
    }

}