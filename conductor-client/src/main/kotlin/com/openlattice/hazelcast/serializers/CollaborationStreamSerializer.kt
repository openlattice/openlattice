package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializers
import com.kryptnostic.rhizome.hazelcast.serializers.UUIDStreamSerializerUtils
import com.openlattice.collaborations.Collaboration
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.mapstores.TestDataFactory
import org.springframework.stereotype.Component

@Component
class CollaborationStreamSerializer : TestableSelfRegisteringStreamSerializer<Collaboration> {

    override fun generateTestValue(): Collaboration {
        return TestDataFactory.collaboration()
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.COLLABORATION.ordinal
    }

    override fun getClazz(): Class<out Collaboration> {
        return Collaboration::class.java
    }

    override fun write(out: ObjectDataOutput, obj: Collaboration) {
        UUIDStreamSerializerUtils.serialize(out, obj.id)
        out.writeUTF(obj.name)
        out.writeUTF(obj.title)
        out.writeUTF(obj.description)
        SetStreamSerializers.fastUUIDSetSerialize(out, obj.organizationIds)
    }

    override fun read(input: ObjectDataInput): Collaboration {
        val id = UUIDStreamSerializerUtils.deserialize(input)
        val name = input.readUTF()
        val title = input.readUTF()
        val description = input.readUTF()
        val organizationIds = SetStreamSerializers.fastOrderedUUIDSetDeserialize(input)

        return Collaboration(id, name, title, description, organizationIds)
    }
}