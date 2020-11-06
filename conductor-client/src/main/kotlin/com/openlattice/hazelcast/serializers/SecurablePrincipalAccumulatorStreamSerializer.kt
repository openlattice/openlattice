package com.openlattice.hazelcast.serializers

import com.google.common.collect.Lists
import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.openlattice.authorization.SecurablePrincipal
import com.openlattice.authorization.mapstores.SecurablePrincipalAccumulator
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.organizations.SecurablePrincipalList
import org.springframework.stereotype.Component


/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Component
class SecurablePrincipalAccumulatorStreamSerializer : TestableSelfRegisteringStreamSerializer<SecurablePrincipalAccumulator> {
    override fun generateTestValue(): SecurablePrincipalAccumulator {
        return SecurablePrincipalAccumulator()
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.SECURABLE_PRINCIPAL_ACCUMULATOR.ordinal
    }

    override fun getClazz(): Class<out SecurablePrincipalAccumulator> {
        return SecurablePrincipalAccumulator::class.java
    }

    override fun write(out: ObjectDataOutput, obj: SecurablePrincipalAccumulator) {
        out.writeInt(obj.v?.size ?: 0)
        obj.v?.forEach { SecurablePrincipalStreamSerializer.serialize(out, it) }
    }

    override fun read(input: ObjectDataInput): SecurablePrincipalAccumulator? {
        val size = input.readInt()
        return if (size == 0) {
            SecurablePrincipalAccumulator()
        } else {
            val list = Lists.newArrayListWithExpectedSize<SecurablePrincipal>(size)
            (0 until size).forEach { _ ->
                list.add(SecurablePrincipalStreamSerializer.deserialize(input))
            }
            SecurablePrincipalAccumulator(SecurablePrincipalList(list))
        }
    }

}