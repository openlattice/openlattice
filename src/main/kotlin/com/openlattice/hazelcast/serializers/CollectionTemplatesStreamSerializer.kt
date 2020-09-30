package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.hazelcast.serializers.UUIDStreamSerializerUtils
import com.openlattice.collections.CollectionTemplates
import com.openlattice.hazelcast.InternalTestDataFactory
import com.openlattice.hazelcast.StreamSerializerTypeIds
import org.springframework.stereotype.Component
import java.util.*

@Component
class CollectionTemplatesStreamSerializer : TestableSelfRegisteringStreamSerializer<CollectionTemplates> {

    companion object {

        fun serialize(out: ObjectDataOutput, `object`: CollectionTemplates) {

            out.writeInt(`object`.templates.size)
            `object`.templates.forEach {
                UUIDStreamSerializerUtils.serialize(out, it.key)
                MapStreamSerializers.writeUUIDUUIDMap(out, it.value)
            }
        }

        fun deserialize(`in`: ObjectDataInput): CollectionTemplates {
            val size = `in`.readInt()

            val templates = mutableMapOf<UUID, MutableMap<UUID, UUID>>()

            for (i in 0 until size) {
                val entitySetCollectionId = UUIDStreamSerializerUtils.deserialize(`in`)
                val templateMap = MapStreamSerializers.readUUIDUUIDMap(`in`)

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

    override fun generateTestValue(): CollectionTemplates {
        return InternalTestDataFactory.collectionTemplates()
    }

}