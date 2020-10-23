package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.hazelcast.serializers.UUIDStreamSerializerUtils
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.mapstores.TestDataFactory
import com.openlattice.postgres.IndexType
import com.openlattice.transporter.types.TransporterColumnSet
import org.springframework.stereotype.Component

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