package com.openlattice.authorization.mapstores

import com.hazelcast.aggregation.Aggregator
import com.openlattice.authorization.AclKey
import com.openlattice.authorization.SecurablePrincipal

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
data class ReadSecurablePrincipalAggregator(
        var sp: SecurablePrincipal? = null
) : Aggregator<MutableMap.MutableEntry<AclKey, SecurablePrincipal>, SecurablePrincipal> {

    override fun accumulate(input: MutableMap.MutableEntry<AclKey, SecurablePrincipal>) {
        sp = input.value
    }

    override fun combine(aggregator: Aggregator<*, *>) {
        val castAggregator = aggregator as ReadSecurablePrincipalAggregator
        if (sp == null) {
            sp = castAggregator.sp
        }
    }

    override fun aggregate(): SecurablePrincipal? {
        return sp
    }
}