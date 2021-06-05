package com.openlattice.authorization.mapstores

import com.hazelcast.aggregation.Aggregator
import com.openlattice.authorization.AclKey
import com.openlattice.authorization.SecurablePrincipal
import com.openlattice.organizations.SecurablePrincipalList
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
data class SecurablePrincipalAccumulator(
        var v: SecurablePrincipalList? = null
) : Aggregator<MutableMap.MutableEntry<AclKey, SecurablePrincipal>, SecurablePrincipalList> {
    override fun accumulate(input: MutableMap.MutableEntry<AclKey, SecurablePrincipal>) {
        if (v == null) {
            v = SecurablePrincipalList(mutableListOf())
        }
        v?.add(input.value)
    }

    @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE")
    override fun combine(aggregator: Aggregator<*, *>) {
        val castAggregator = aggregator as SecurablePrincipalAccumulator
        if (v == null) {
            v = castAggregator.v
        } else if (castAggregator.v != null) {
            v?.addAll(castAggregator.v!!)
        }
    }

    override fun aggregate(): SecurablePrincipalList? {
        return v
    }
}