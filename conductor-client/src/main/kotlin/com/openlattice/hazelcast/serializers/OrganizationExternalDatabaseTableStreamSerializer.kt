package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.hazelcast.serializers.UUIDStreamSerializerUtils
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.organization.OrganizationExternalDatabaseTable
import org.springframework.stereotype.Component
import java.util.*

@Component
class OrganizationExternalDatabaseTableStreamSerializer : SelfRegisteringStreamSerializer<OrganizationExternalDatabaseTable> {

    companion object {
        fun serialize(output: ObjectDataOutput, obj: OrganizationExternalDatabaseTable) {
            UUIDStreamSerializerUtils.serialize(output, obj.id)
            output.writeUTF(obj.name)
            output.writeUTF(obj.title)
            output.writeUTF(obj.description)
            UUIDStreamSerializerUtils.serialize(output, obj.organizationId)
            output.writeInt(obj.oid)
        }

        fun deserialize(input: ObjectDataInput): OrganizationExternalDatabaseTable {
            val id = UUIDStreamSerializerUtils.deserialize(input)
            val name = input.readUTF()
            val title = input.readUTF()
            val description = input.readUTF()
            val orgId = UUIDStreamSerializerUtils.deserialize(input)
            val oid = input.readInt();
            return OrganizationExternalDatabaseTable(id, name, title, Optional.of(description), orgId, oid)
        }
    }

    override fun write(output: ObjectDataOutput, obj: OrganizationExternalDatabaseTable) {
        serialize(output, obj)
    }

    override fun read(input: ObjectDataInput): OrganizationExternalDatabaseTable {
        return deserialize(input)
    }

    override fun getClazz(): Class<out OrganizationExternalDatabaseTable> {
        return OrganizationExternalDatabaseTable::class.java
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.ORGANIZATION_EXTERNAL_DATABASE_TABLE.ordinal
    }

}