package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializers
import com.kryptnostic.rhizome.hazelcast.serializers.UUIDStreamSerializerUtils
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer
import com.openlattice.collections.EntitySetCollection
import com.openlattice.hazelcast.StreamSerializerTypeIds
import org.springframework.stereotype.Component
import java.util.*

@Component
class EntitySetCollectionStreamSerializer : SelfRegisteringStreamSerializer<EntitySetCollection> {
    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.ENTITY_SET_COLLECTION.ordinal
    }

    override fun getClazz(): Class<out EntitySetCollection> {
        return EntitySetCollection::class.java
    }

    override fun write(out: ObjectDataOutput?, `object`: EntitySetCollection) {
        val collection = `object`

        UUIDStreamSerializerUtils.serialize(out, collection.id)
        out?.writeUTF(collection.name)
        out?.writeUTF(collection.title)
        out?.writeUTF(collection.description)
        UUIDStreamSerializerUtils.serialize(out, collection.entityTypeCollectionId)
        SetStreamSerializers.serialize(out, collection.contacts, ObjectDataOutput::writeUTF)
        UUIDStreamSerializerUtils.serialize(out, collection.organizationId)

        out?.writeInt(collection.template.size)
        collection.template.forEach { typeId, entitySetId ->
            run {
                UUIDStreamSerializerUtils.serialize(out, typeId)
                UUIDStreamSerializerUtils.serialize(out, entitySetId)
            }
        }
    }

    override fun read(`in`: ObjectDataInput?): EntitySetCollection {
        val input = `in`!!

        val id = UUIDStreamSerializerUtils.deserialize(input)
        val name = input.readUTF()
        val title = input.readUTF()
        val description = Optional.of(input.readUTF())
        val entityTypeCollectionId = UUIDStreamSerializerUtils.deserialize(input)
        val contacts = SetStreamSerializers.deserialize(input, ObjectDataInput::readUTF)
        val organizationId = UUIDStreamSerializerUtils.deserialize(input)

        val templateSize = input.readInt()
        val template = mutableMapOf<UUID, UUID>()
        for (i in 0 until templateSize) {
            val typeId = UUIDStreamSerializerUtils.deserialize(input)
            val entitySetId = UUIDStreamSerializerUtils.deserialize(input)
            template[typeId] = entitySetId
        }

        return EntitySetCollection(id, name, title, description, entityTypeCollectionId, template, contacts, organizationId)
    }

}