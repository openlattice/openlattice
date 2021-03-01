package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.openlattice.hazelcast.StreamSerializerTypeIds
import org.springframework.stereotype.Component
import java.util.UUID

/**
 * @author Drew Bailey (drew@openlattice.com)
 */
@Component
class UUIDStreamSerializer: TestableSelfRegisteringStreamSerializer<UUID> {
    companion object {
        @JvmStatic
        fun serialize(out: ObjectDataOutput, `object`: UUID) {
            out.writeLong(`object`.mostSignificantBits)
            out.writeLong(`object`.leastSignificantBits)
        }

        @JvmStatic
        fun deserialize(`in`: ObjectDataInput): UUID {
            return UUID( `in`.readLong(), `in`.readLong())
        }
    }

    override fun getClazz(): Class<out UUID> {
        return UUID::class.java
    }

    override fun write(out: ObjectDataOutput, `object`: UUID) {
        serialize(out, `object`)
    }

    override fun read(`in`: ObjectDataInput): UUID {
        return deserialize(`in`)
    }

    override fun generateTestValue(): UUID {
        return UUID.randomUUID()
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.UUID.ordinal
    }
}
