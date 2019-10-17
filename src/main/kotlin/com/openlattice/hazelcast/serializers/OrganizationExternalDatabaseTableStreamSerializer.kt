package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.organization.OrganizationExternalDatabaseTable
import org.springframework.stereotype.Component
import java.util.*

@Component
class OrganizationExternalDatabaseTableStreamSerializer : SelfRegisteringStreamSerializer<OrganizationExternalDatabaseTable> {

    companion object {
        fun serialize(output: ObjectDataOutput, obj: OrganizationExternalDatabaseTable) {
            UUIDStreamSerializer.serialize(output, obj.id)
            output.writeUTF(obj.name)
            output.writeUTF(obj.title)
            output.writeUTF(obj.description)
            UUIDStreamSerializer.serialize(output, obj.organizationId)
        }

        fun deserialize(input: ObjectDataInput): OrganizationExternalDatabaseTable {
            val id = UUIDStreamSerializer.deserialize(input)
            val name = input.readUTF()
            val title = input.readUTF()
            val description = input.readUTF()
            val orgId = UUIDStreamSerializer.deserialize(input)
            return OrganizationExternalDatabaseTable(id, name, title, Optional.of(description), orgId)
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