package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer
import com.openlattice.authorization.Permission
import com.openlattice.data.DataExpiration
import com.openlattice.edm.set.ExpirationType
import com.openlattice.hazelcast.StreamSerializerTypeIds
import org.springframework.stereotype.Component
import java.util.*

@Component
class DataExpirationStreamSerializer : SelfRegisteringStreamSerializer<DataExpiration> {
    val expirationTypes = ExpirationType.values()

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.DATA_EXPIRATION.ordinal
    }

    override fun destroy() {}

    override fun getClazz(): Class<out DataExpiration> {
        return DataExpiration::class.java
    }

    override fun write(out: ObjectDataOutput, obj: DataExpiration) {
        out.writeLong(obj.timeToExpiration)
        out.writeInt(obj.expirationFlag.ordinal)
        OptionalStreamSerializers.serialize(out, obj.startDateProperty, UUIDStreamSerializer::serialize)
    }

    override fun read(input: ObjectDataInput): DataExpiration {
        val timeToExpiration = input.readLong()
        val expirationType = expirationTypes[input.readInt()]
        val startDateProperty = OptionalStreamSerializers.deserialize(input, UUIDStreamSerializer::deserialize)
        return DataExpiration(timeToExpiration, expirationType, startDateProperty)
    }
}