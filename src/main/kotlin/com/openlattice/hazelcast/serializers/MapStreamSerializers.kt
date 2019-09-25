package com.openlattice.hazelcast.serializers

import com.google.common.collect.Maps
import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializers
import java.io.IOException
import java.util.*

class MapStreamSerializers {

    companion object {

        @Throws(IOException::class)
        fun writeUUIDUUIDMap(out: ObjectDataOutput, `object`: Map<UUID, UUID>) {
            SetStreamSerializers.fastUUIDSetSerialize(out, `object`.keys)
            SetStreamSerializers.fastUUIDSetSerialize(out, `object`.values)
        }

        @Throws(IOException::class)
        fun readUUIDUUIDMap(`in`: ObjectDataInput, defaultMap: MutableMap<UUID, UUID>?): Map<UUID, UUID> {
            val keys = SetStreamSerializers.fastOrderedUUIDSetDeserialize(`in`)
            val vals = SetStreamSerializers.fastOrderedUUIDSetDeserialize(`in`)

            val keyIt = keys.iterator()
            val valIt = vals.iterator()

            val map = defaultMap ?: Maps.newHashMapWithExpectedSize(keys.size)
            while (keyIt.hasNext()) {
                map[keyIt.next()] = valIt.next()
            }

            return map
        }
    }
}