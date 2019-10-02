package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer
import com.openlattice.data.DataExpiration
import com.openlattice.data.DeleteType
import com.openlattice.edm.set.ExpirationBase
import com.openlattice.hazelcast.StreamSerializerTypeIds
import org.springframework.stereotype.Component

@Component
class DataExpirationStreamSerializer : SelfRegisteringStreamSerializer<DataExpiration> {

    companion object {
        private val expirationTypes = ExpirationBase.values()
        private val deleteTypes = DeleteType.values()

        @JvmStatic
        fun serialize(out: ObjectDataOutput, obj: DataExpiration) {
            out.writeLong(obj.timeToExpiration)
            out.writeInt(obj.expirationBase.ordinal)
            out.writeInt(obj.deleteType.ordinal)
            OptionalStreamSerializers.serialize(out, obj.startDateProperty, UUIDStreamSerializer::serialize)
        }

        @JvmStatic
        fun deserialize(input: ObjectDataInput): DataExpiration {
            val timeToExpiration = input.readLong()
            val expirationType = expirationTypes[input.readInt()]
            val deleteType = deleteTypes[input.readInt()]
            val startDateProperty = OptionalStreamSerializers.deserialize(input, UUIDStreamSerializer::deserialize)
            return DataExpiration(timeToExpiration, expirationType, deleteType, startDateProperty)
        }

    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.DATA_EXPIRATION.ordinal
    }

    override fun destroy() {}

    override fun getClazz(): Class<out DataExpiration> {
        return DataExpiration::class.java
    }

    override fun write(out: ObjectDataOutput, obj: DataExpiration) {
        serialize(out, obj)
    }

    override fun read(input: ObjectDataInput): DataExpiration {
        return deserialize(input)
    }
}