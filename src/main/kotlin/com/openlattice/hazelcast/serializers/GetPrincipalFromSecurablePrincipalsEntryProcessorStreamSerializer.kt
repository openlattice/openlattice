package com.openlattice.hazelcast.serializers

import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.hazelcast.processors.GetPrincipalFromSecurablePrincipalsEntryProcessor
import org.springframework.stereotype.Component

/**
 * @author Drew Bailey &lt;drew@openlattice.com&gt;
 */
@Component
class GetPrincipalFromSecurablePrincipalsEntryProcessorStreamSerializer:
        NoOpSelfRegisteringStreamSerializer<GetPrincipalFromSecurablePrincipalsEntryProcessor>() {

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.GET_PRINCIPAL_FROM_SECURABLE_TYPE_EP.ordinal
    }

    override fun getClazz(): Class<out GetPrincipalFromSecurablePrincipalsEntryProcessor> {
        return GetPrincipalFromSecurablePrincipalsEntryProcessor::class.java
    }
}