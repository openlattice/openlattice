package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.hazelcast.serializers.UUIDStreamSerializerUtils
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.organization.OrganizationExternalDatabaseColumn
import org.springframework.stereotype.Component
import java.util.*

@Component
class OrganizationExternalDatabaseColumnStreamSerializer : SelfRegisteringStreamSerializer<OrganizationExternalDatabaseColumn> {

    companion object {
        fun serialize(output: ObjectDataOutput, obj: OrganizationExternalDatabaseColumn) {
            UUIDStreamSerializerUtils.serialize(output, obj.id)
            output.writeUTF(obj.name)
            output.writeUTF(obj.title)
            output.writeUTF(obj.description)
            output.writeUTF(obj.externalId)
            UUIDStreamSerializerUtils.serialize(output, obj.tableId)
            UUIDStreamSerializerUtils.serialize(output, obj.organizationId)
            UUIDStreamSerializerUtils.serialize(output, obj.dataSourceId)
            output.writeUTF(obj.dataType)
            output.writeBoolean(obj.primaryKey)
            output.writeInt(obj.ordinalPosition)
        }

        fun deserialize(input: ObjectDataInput): OrganizationExternalDatabaseColumn {
            val id = UUIDStreamSerializerUtils.deserialize(input)
            val name = input.readUTF()
            val title = input.readUTF()
            val description = input.readUTF()
            val externalId = input.readUTF()
            val tableId = UUIDStreamSerializerUtils.deserialize(input)
            val orgId = UUIDStreamSerializerUtils.deserialize(input)
            val dataSourceId = UUIDStreamSerializerUtils.deserialize(input)
            val dataType = input.readUTF()
            val isPrimaryKey = input.readBoolean()
            val ordinalPosition = input.readInt()
            return OrganizationExternalDatabaseColumn(
                    id,
                    name,
                    title,
                    Optional.of(description),
                    externalId,
                    tableId,
                    orgId,
                    dataSourceId,
                    dataType,
                    isPrimaryKey,
                    ordinalPosition
            )
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