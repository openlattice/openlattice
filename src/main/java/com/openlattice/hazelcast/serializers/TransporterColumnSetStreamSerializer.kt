package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractUUIDStreamSerializer
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.mapstores.TestDataFactory
import com.openlattice.postgres.IndexType
import com.openlattice.postgres.PostgresDatatype
import com.openlattice.transporter.types.TransporterColumn
import com.openlattice.transporter.types.TransporterColumnSet
import org.springframework.stereotype.Component

@Component
class TransporterColumnSetStreamSerializer : TestableSelfRegisteringStreamSerializer<TransporterColumnSet> {
    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.TRANSPORTER_COLUMN_SET.ordinal
    }

    override fun getClazz(): Class<out TransporterColumnSet> {
        return TransporterColumnSet::class.java
    }

    override fun write(out: ObjectDataOutput, `object`: TransporterColumnSet) {
        out.writeInt(`object`.size)
        `object`.entries.forEach { (id, col) ->
            AbstractUUIDStreamSerializer.serialize(out, id)
            out.writeUTF(col.srcCol)
            out.writeUTF(col.destColName)
            AbstractEnumSerializer.serialize(out, col.dataType)
        }
    }

    override fun read(`in`: ObjectDataInput): TransporterColumnSet {
        val size = `in`.readInt()
        val columns = (1..size).map { _ ->
            val uuid = AbstractUUIDStreamSerializer.deserialize(`in`)
            val srcCol = `in`.readUTF()
            val destColName = `in`.readUTF()
            val dataType = AbstractEnumSerializer.deserialize(PostgresDatatype::class.java, `in`)
            uuid to TransporterColumn(srcCol, destColName, dataType)
        }.toMap()
        return TransporterColumnSet(columns)
    }

    override fun generateTestValue(): TransporterColumnSet {
        return TransporterColumnSet(mapOf())
                .withProperties(listOf(
                        TestDataFactory.propertyType(IndexType.NONE, false),
                        TestDataFactory.propertyType(IndexType.BTREE, true)
        ))
    }
}