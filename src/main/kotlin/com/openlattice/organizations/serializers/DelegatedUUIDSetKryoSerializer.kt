package com.openlattice.organizations.serializers

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.Serializer
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.io.Output
import com.openlattice.rhizome.hazelcast.DelegatedUUIDSet
import java.util.*

class DelegatedUUIDSetKryoSerializer: Serializer<DelegatedUUIDSet>() {

    override fun write(kryo: Kryo?, output: Output?, `object`: DelegatedUUIDSet?) {
        output?.writeInt( `object`!!.size )
        `object`!!.forEach {
            output!!.writeLong( it.leastSignificantBits )
            output!!.writeLong( it.mostSignificantBits )
        }
    }

    override fun read(kryo: Kryo?, input: Input?, type: Class<DelegatedUUIDSet>?): DelegatedUUIDSet {
        val size = input!!.readInt()
        val uuidSet = DelegatedUUIDSet.wrap( HashSet<UUID>( size ) )
        for( i in 1..size ) {
            val uuid = UUID( input.readLong(), input.readLong() )
            uuidSet.add( uuid )
        }
        return uuidSet
    }
}