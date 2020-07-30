package com.openlattice.hazelcast.serializers

import com.google.common.collect.Maps
import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.hazelcast.serializers.UUIDStreamSerializerUtils
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer
import com.openlattice.collections.CollectionTemplates
import com.openlattice.hazelcast.StreamSerializerTypeIds
import org.springframework.stereotype.Component
import java.util.*
import java.util.concurrent.ConcurrentMap

@Component
class CollectionTemplatesStreamSerializer : SelfRegisteringStreamSerializer<CollectionTemplates> {

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

            val templates = Maps.newConcurrentMap<UUID, ConcurrentMap<UUID, UUID>>()

            for (i in 0 until size) {

                val entitySetCollectionId = UUIDStreamSerializerUtils.deserialize(`in`)
                val templateMap = MapStreamSerializers.readUUIDUUIDMap(`in`, Maps.newConcurrentMap()) as ConcurrentMap<UUID, UUID>

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