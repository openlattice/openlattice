package com.openlattice.organizations.serializers

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.Serializer
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.io.Output
import com.openlattice.authorization.Principal
import com.openlattice.authorization.PrincipalType
import com.openlattice.organizations.PrincipalSet

class PrincipalSetKryoSerializer:Serializer<PrincipalSet>() {
    private val principalTypes = PrincipalType.values()

    override fun write(kryo: Kryo?, output: Output?, `object`: PrincipalSet?) {
        output?.writeInt( `object`!!.size )
        `object`!!.forEach {
            output!!.writeInt( it.type.ordinal )
            output!!.writeString( it.id )
        }
    }

    override fun read(kryo: Kryo?, input: Input?, type: Class<PrincipalSet>?): PrincipalSet {
        val size = input!!.readInt()
        val principalSet = PrincipalSet.wrap( HashSet<Principal>( size ) )
        for( i in 1..size ) {
            val principal = Principal( principalTypes[input.readInt()], input.readString() )
            principalSet.add( principal )
        }
        return principalSet
    }
}