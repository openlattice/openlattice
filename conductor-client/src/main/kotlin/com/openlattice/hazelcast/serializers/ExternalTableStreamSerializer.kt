package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.hazelcast.serializers.UUIDStreamSerializerUtils
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.mapstores.TestDataFactory
import com.openlattice.organization.ExternalTable
import org.springframework.stereotype.Component
import java.util.*

@Component
class ExternalTableStreamSerializer : TestableSelfRegisteringStreamSerializer<ExternalTable> {

    companion object {
        fun serialize(output: ObjectDataOutput, obj: ExternalTable) {
            UUIDStreamSerializerUtils.serialize(output, obj.id)
            output.writeUTF(obj.name)
            output.writeUTF(obj.title)
            output.writeUTF(obj.description)
            UUIDStreamSerializerUtils.serialize(output, obj.organizationId)
            output.writeLong(obj.oid)
            output.writeUTF(obj.schema)
        }

        fun deserialize(input: ObjectDataInput): ExternalTable {
            val id = UUIDStreamSerializerUtils.deserialize(input)
            val name = input.readUTF()
            val title = input.readUTF()
            val description = input.readUTF()
            val orgId = UUIDStreamSerializerUtils.deserialize(input)
            val oid = input.readLong()
            val schema = input.readUTF()
            return ExternalTable(id, name, title, Optional.of(description), orgId, oid, schema)
        }
    }

    override fun write(output: ObjectDataOutput, obj: ExternalTable) {
        serialize(output, obj)
    }

    override fun read(input: ObjectDataInput): ExternalTable {
        return deserialize(input)
    }

    override fun getClazz(): Class<out ExternalTable> {
        return ExternalTable::class.java
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.EXTERNAL_TABLE.ordinal
    }

    override fun generateTestValue(): ExternalTable {
        return TestDataFactory.externalTable()
    }
}
