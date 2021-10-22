package com.openlattice.hazelcast.serializers

class TransporterColumnStreamSerializer

/**
 * @author Drew Bailey &lt;drew@openlattice.com&gt;
 */
/*
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
            val srcCol = `in`.readString()!!
            val destColName = `in`.readString()!!
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
*/
