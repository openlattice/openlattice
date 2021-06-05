package com.openlattice.hazelcast.serializers

import com.google.common.collect.Sets
import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.rhizome.DelegatedIntSet
import org.springframework.stereotype.Component

/**
 * @author Drew Bailey &lt;drew@openlattice.com&gt;
 */
@Component
class DelegatedIntSetStreamSerializer : TestableSelfRegisteringStreamSerializer<DelegatedIntSet> {
    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.INT_SET.ordinal
    }

    override fun getClazz(): Class<out DelegatedIntSet> {
        return DelegatedIntSet::class.java
    }

    override fun write(out: ObjectDataOutput, `object`: DelegatedIntSet) {
        out.writeInt(`object`.size)
        `object`.forEach { out.writeInt(it) }
    }

    override fun read(`in`: ObjectDataInput): DelegatedIntSet {
        val size = `in`.readInt()
        val set = Sets.newLinkedHashSetWithExpectedSize<Int>( size )
        for ( i in 0 until size ) {
            set.add(`in`.readInt())
        }
        return DelegatedIntSet( set )
    }

    override fun generateTestValue(): DelegatedIntSet {
        return DelegatedIntSet(setOf(1, 2, 3, 4))
    }

}