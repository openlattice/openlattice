package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.mapstores.TestDataFactory
import com.openlattice.postgres.PostgresDatatype
import com.openlattice.transporter.types.TransporterColumn
import org.springframework.stereotype.Component

/**
 * @author Drew Bailey &lt;drew@openlattice.com&gt;
 */
@Component
class TransporterColumnStreamSerializer: TestableSelfRegisteringStreamSerializer<TransporterColumn> {

    companion object {
        @JvmStatic
        fun serializeColumn(out: ObjectDataOutput, col: TransporterColumn) {
            out.writeUTF(col.dataTableColumnName)
            out.writeUTF(col.transporterTableColumnName)
            AbstractEnumSerializer.serialize(out, col.dataType)
        }

        @JvmStatic
        fun deserializeColumn(`in`: ObjectDataInput): TransporterColumn {
            val srcCol = `in`.readUTF()
            val destColName = `in`.readUTF()
            val dataType = AbstractEnumSerializer.deserialize(PostgresDatatype::class.java, `in`)
            return TransporterColumn(srcCol, destColName, dataType)
        }
    }

    override fun write(out: ObjectDataOutput, col: TransporterColumn) {
        serializeColumn(out, col)
    }

    override fun read(`in`: ObjectDataInput): TransporterColumn {
        return deserializeColumn(`in`)
    }

    override fun generateTestValue(): TransporterColumn {
        return TransporterColumn(
                TestDataFactory.randomAlphabetic(10),
                TestDataFactory.randomAlphabetic(10),
                PostgresDatatype.NUMERIC
        )
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.TRANSPORTER_COLUMN.ordinal
    }

    override fun getClazz(): Class<out TransporterColumn> {
        return TransporterColumn::class.java
    }

}