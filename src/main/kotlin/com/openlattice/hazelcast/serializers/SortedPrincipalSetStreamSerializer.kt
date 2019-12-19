package com.openlattice.hazelcast.serializers

import com.google.common.collect.Sets
import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.mapstores.TestDataFactory
import com.openlattice.organizations.SortedPrincipalSet
import org.springframework.stereotype.Component

@Component
class SortedPrincipalSetStreamSerializer : TestableSelfRegisteringStreamSerializer<SortedPrincipalSet> {
    override fun generateTestValue(): SortedPrincipalSet {
        return SortedPrincipalSet(Sets.newTreeSet((0 until 3).map { TestDataFactory.userPrincipal() }))
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.SORTED_PRINCIPAL_SET.ordinal
    }

    override fun getClazz(): Class<out SortedPrincipalSet> {
        return SortedPrincipalSet::class.java
    }

    override fun write(out: ObjectDataOutput, `object`: SortedPrincipalSet) {
        out.writeInt(`object`.size)
        `object`.forEach { PrincipalStreamSerializer.serialize(out, it) }
    }

    override fun read(input: ObjectDataInput): SortedPrincipalSet {
        val size = input.readInt()
        return SortedPrincipalSet(Sets.newTreeSet(
                (0 until size).map { PrincipalStreamSerializer.deserialize(input) }
        ))
    }
}