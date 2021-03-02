package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import java.util.*

/**
 * @author Drew Bailey (drew@openlattice.com)
 */
class UUIDStreamSerializer {
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
}
