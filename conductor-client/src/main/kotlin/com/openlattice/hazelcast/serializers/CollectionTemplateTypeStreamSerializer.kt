package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.geekbeast.rhizome.hazelcast.serializers.UUIDStreamSerializerUtils
import com.geekbeast.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer
import com.openlattice.collections.CollectionTemplateType
import com.openlattice.hazelcast.StreamSerializerTypeIds
import org.springframework.stereotype.Component
import java.util.*

@Component
class CollectionTemplateTypeStreamSerializer : SelfRegisteringStreamSerializer<CollectionTemplateType> {

    companion object {
        fun serialize(out: ObjectDataOutput, `object`: CollectionTemplateType) {
            UUIDStreamSerializerUtils.serialize(out, `object`.id)
            out.writeUTF(`object`.name)
            out.writeUTF(`object`.title)
            out.writeUTF(`object`.description)
            UUIDStreamSerializerUtils.serialize(out, `object`.entityTypeId)
        }

        fun deserialize(`in`: ObjectDataInput): CollectionTemplateType {
            val id = UUIDStreamSerializerUtils.deserialize(`in`)
            val name = `in`.readString()!!
            val title = `in`.readString()!!
            val description = `in`.readString()!!
            val entityTypeId = UUIDStreamSerializerUtils.deserialize(`in`)

            return CollectionTemplateType(id, name, title, Optional.of(description), entityTypeId)
        }
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.COLLECTION_TEMPLATE_TYPE.ordinal
    }

    override fun getClazz(): Class<out CollectionTemplateType> {
        return CollectionTemplateType::class.java
    }

    override fun write(out: ObjectDataOutput, `object`: CollectionTemplateType) {
        serialize(out, `object`)
    }

    override fun read(`in`: ObjectDataInput): CollectionTemplateType {
        return deserialize(`in`)
    }

}