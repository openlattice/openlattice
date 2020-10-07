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
        val least = LongArray(`object`.size)
        val most = LongArray(`object`.size)
        for ((i, uuid) in `object`.withIndex()) {
            least[i] = uuid.leastSignificantBits
            most[i] = uuid.mostSignificantBits
        }
        out.writeInt(`object`.size)
        out.writeLongArray(least)
        out.writeLongArray(most)
    }

    override fun read(`in`: ObjectDataInput): KotlinDelegatedUUIDSet {
        val size = `in`.readInt()
        if ( size == 0 ) {
            return KotlinDelegatedUUIDSet(setOf())
        }

        val set = Sets.newLinkedHashSetWithExpectedSize<UUID>(size)
        val least = `in`.readLongArray()
        val most = `in`.readLongArray()
        for ((i, long) in least.withIndex()){
            set.add(UUID(most[i], long))
        }

        return KotlinDelegatedUUIDSet(set)
    }
}