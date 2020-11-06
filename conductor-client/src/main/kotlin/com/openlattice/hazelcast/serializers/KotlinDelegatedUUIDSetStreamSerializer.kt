package com.openlattice.hazelcast.serializers

import com.google.common.collect.Sets
import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.rhizome.KotlinDelegatedUUIDSet
import org.springframework.stereotype.Component
import java.util.*

/**
 * @author Drew Bailey &lt;drew@openlattice.com&gt;
 */
@Component
class KotlinDelegatedUUIDSetStreamSerializer: TestableSelfRegisteringStreamSerializer<KotlinDelegatedUUIDSet>{
    override fun generateTestValue(): KotlinDelegatedUUIDSet {
        return KotlinDelegatedUUIDSet(
                setOf(
                        UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(),
                        UUID.randomUUID(), UUID.randomUUID()))
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.KOTLIN_DELEGATED_UUID_SET.ordinal
    }

    override fun getClazz(): Class<out KotlinDelegatedUUIDSet> {
        return KotlinDelegatedUUIDSet::class.java
    }

    override fun write(out: ObjectDataOutput, `object`: KotlinDelegatedUUIDSet) {
        out.writeInt( `object`.size )
        `object`.forEach { id ->
            out.writeLong( id.mostSignificantBits )
            out.writeLong( id.leastSignificantBits )
        }
    }

    override fun read(`in`: ObjectDataInput): KotlinDelegatedUUIDSet {
        val size = `in`.readInt()
        if ( size == 0 ) {
            return KotlinDelegatedUUIDSet(setOf())
        }

        val set = Sets.newLinkedHashSetWithExpectedSize<UUID>(size)
        repeat( size ) {
            set.add( UUID( `in`.readLong(), `in`.readLong()))
        }

        return KotlinDelegatedUUIDSet( set )
    }
}