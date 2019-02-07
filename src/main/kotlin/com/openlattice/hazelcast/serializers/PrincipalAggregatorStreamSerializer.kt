package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer
import com.openlattice.authorization.AclKey
import com.openlattice.authorization.aggregators.PrincipalAggregator
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.organizations.PrincipalSet
import org.springframework.stereotype.Component

@Component
class PrincipalAggregatorStreamSerializer : SelfRegisteringStreamSerializer<PrincipalAggregator> {
    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.PRINCIPAL_AGGREGATOR.ordinal
    }

    override fun destroy() {}

    override fun getClazz(): Class<out PrincipalAggregator> {
        return PrincipalAggregator::class.java
    }

    override fun write(output: ObjectDataOutput, value: PrincipalAggregator) {
        output.writeInt(value.getResult().size)
        value.getResult().forEach {
            AclKeyStreamSerializer.serialize(output, it.key);
            PrincipalSetStreamSerializer().write(output, it.value)
        }
    }

    override fun read(input: ObjectDataInput): PrincipalAggregator {
        val size = input.readInt()
        val principalMap = HashMap<AclKey, PrincipalSet>(size)
        (1..size).forEach {
            val key = AclKeyStreamSerializer.deserialize(input)
            val principals = PrincipalSetStreamSerializer().read(input)
            principalMap[key] = principals
        }

        return PrincipalAggregator(principalMap)
    }
}