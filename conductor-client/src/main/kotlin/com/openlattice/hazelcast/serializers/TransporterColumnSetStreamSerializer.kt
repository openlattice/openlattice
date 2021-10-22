package com.openlattice.hazelcast.serializers

class TransporterColumnSetStreamSerializer

/*
@Component
class TransporterColumnSetStreamSerializer : TestableSelfRegisteringStreamSerializer<TransporterColumnSet> {

    companion object {
        @JvmStatic
        fun serializeColumnSet(out: ObjectDataOutput, columnSet: TransporterColumnSet) {
            out.writeInt(columnSet.size)
            columnSet.entries.forEach { (id, col) ->
                UUIDStreamSerializerUtils.serialize(out, id)
                TransporterColumnStreamSerializer.serializeColumn(out, col)
            }
        }

        @JvmStatic
        fun deserializeColumnSet(`in`: ObjectDataInput): TransporterColumnSet {
            val size = `in`.readInt()
            val columns = (1..size).associate { _ ->
                UUIDStreamSerializerUtils.deserialize(`in`) to
                        TransporterColumnStreamSerializer.deserializeColumn(`in`)
            }
            return TransporterColumnSet(columns)
        }
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.TRANSPORTER_COLUMN_SET.ordinal
    }

    override fun getClazz(): Class<out TransporterColumnSet> {
        return TransporterColumnSet::class.java
    }

    override fun write(out: ObjectDataOutput, `object`: TransporterColumnSet) {
        serializeColumnSet(out, `object`)
    }

    override fun read(`in`: ObjectDataInput): TransporterColumnSet {
        return deserializeColumnSet(`in`)
    }

    override fun generateTestValue(): TransporterColumnSet {
        return TransporterColumnSet(mapOf())
                .withProperties(listOf(
                        TestDataFactory.propertyType(IndexType.NONE, false),
                        TestDataFactory.propertyType(IndexType.BTREE, true)
        ))
    }
}
*/
