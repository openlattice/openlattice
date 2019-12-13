package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.openlattice.authorization.mapstores.ReadSecurablePrincipalAggregator
import com.openlattice.hazelcast.StreamSerializerTypeIds
import org.springframework.stereotype.Component

@Component
class ReadSecurablePrincipalAggregatorStreamSerializer : TestableSelfRegisteringStreamSerializer<ReadSecurablePrincipalAggregator> {

    override fun generateTestValue(): ReadSecurablePrincipalAggregator {
        return ReadSecurablePrincipalAggregator()
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.READ_SECURABLE_PRINCIPAL_AGGREGATOR.ordinal
    }

    override fun getClazz(): Class<out ReadSecurablePrincipalAggregator> {
        return ReadSecurablePrincipalAggregator::class.java
    }

    override fun write(out: ObjectDataOutput, `object`: ReadSecurablePrincipalAggregator) {
        out.writeBoolean(`object`.sp != null)
        if (`object`.sp != null) {
            SecurablePrincipalStreamSerializer.serialize(out, `object`.sp)
        }
    }

    override fun read(input: ObjectDataInput): ReadSecurablePrincipalAggregator {
        return if (input.readBoolean()) {
            ReadSecurablePrincipalAggregator(SecurablePrincipalStreamSerializer.deserialize(input))
        } else {
            ReadSecurablePrincipalAggregator()
        }
    }
}