package com.openlattice.organizations.serializers

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.Serializer
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.io.Output
import com.openlattice.rhizome.hazelcast.DelegatedStringSet

class DelegatedStringSetKryoSerializer: Serializer<DelegatedStringSet>() {

    override fun write(kryo: Kryo?, output: Output?, `object`: DelegatedStringSet?) {
        output?.writeInt( `object`!!.size )
        `object`!!.forEach {
            output!!.writeString( it )
        }
    }

    override fun read(kryo: Kryo?, input: Input?, type: Class<DelegatedStringSet>?): DelegatedStringSet {
        val size = input!!.readInt()
        val stringSet = DelegatedStringSet.wrap( HashSet<String>( size ) )
        for( i in 1..size ) stringSet.add( input.readString() )
        return stringSet
    }
}