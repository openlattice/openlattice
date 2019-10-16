package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.organization.OrganizationExternalDatabaseColumn
import java.util.*

class OrganizationExternalDatabaseColumnStreamSerializer : SelfRegisteringStreamSerializer<OrganizationExternalDatabaseColumn> {

    companion object {
        fun serialize(output: ObjectDataOutput, obj: OrganizationExternalDatabaseColumn) {
            UUIDStreamSerializer.serialize(output, obj.id)
            output.writeUTF(obj.name)
            output.writeUTF(obj.title)
            output.writeUTF(obj.description)
            UUIDStreamSerializer.serialize(output, obj.tableId)
            UUIDStreamSerializer.serialize(output, obj.organizationId)
        }

        fun deserialize(input: ObjectDataInput): OrganizationExternalDatabaseColumn {
            val id = UUIDStreamSerializer.deserialize(input)
            val name = input.readUTF()
            val title = input.readUTF()
            val description = input.readUTF()
            val tableId = UUIDStreamSerializer.deserialize(input)
            val orgId = UUIDStreamSerializer.deserialize(input)
            return OrganizationExternalDatabaseColumn(id, name, title, Optional.of(description), tableId, orgId)
        }
    }

    override fun write(output: ObjectDataOutput, obj: OrganizationExternalDatabaseColumn) {
        serialize(output, obj)
    }

    override fun read(input: ObjectDataInput): OrganizationExternalDatabaseColumn {
        return deserialize(input)
    }

    override fun getClazz(): Class<out OrganizationExternalDatabaseColumn> {
        return OrganizationExternalDatabaseColumn::class.java
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.ORGANIZATION_EXTERNAL_DATABASE_COLUMN.ordinal
    }

}