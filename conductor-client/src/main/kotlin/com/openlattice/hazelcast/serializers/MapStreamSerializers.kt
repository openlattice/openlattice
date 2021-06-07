package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.hazelcast.serializers.ListStreamSerializers
import org.slf4j.LoggerFactory
import java.io.IOException
import java.util.*

class MapStreamSerializers {

    companion object {

        @Throws(IOException::class)
        fun writeUUIDUUIDMap(out: ObjectDataOutput, `object`: Map<UUID, UUID>) {
            val (keys, values) = `object`.asSequence().map { it.toPair() }.unzip()
            ListStreamSerializers.fastUUIDListSerialize(out, keys)
            ListStreamSerializers.fastUUIDListSerialize(out, values)
        }

        @Throws(IOException::class)
        fun readUUIDUUIDMap(`in`: ObjectDataInput): MutableMap<UUID, UUID> {
            val keys = ListStreamSerializers.fastUUIDListDeserialize(`in`)
            val vals = ListStreamSerializers.fastUUIDListDeserialize(`in`)
            if (keys.size != vals.size) {
                LoggerFactory.getLogger(MapStreamSerializers::class.java).error("${keys.size} keys but only ${vals.size} values.")
                throw IllegalStateException("THIS SHOULD NOT HAPPEN")
            }
            return keys.zip(vals).toMap(mutableMapOf())
        }
    }
}