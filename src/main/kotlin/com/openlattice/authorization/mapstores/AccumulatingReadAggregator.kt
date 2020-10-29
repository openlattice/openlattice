package com.openlattice.authorization.mapstores

import com.hazelcast.aggregation.Aggregator
import com.openlattice.authorization.AclKeySet
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class AccumulatingReadAggregator<K> : Aggregator<MutableMap.MutableEntry<K, AclKeySet>, AclKeySet> {
    private var v: AclKeySet? = null
    override fun accumulate(input: MutableMap.MutableEntry<K, AclKeySet>) {
        if (v == null) {
            v = AclKeySet(input.value)
        }
        v?.addAll(input.value)
    }

    @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE")
    override fun combine(aggregator: Aggregator<*, *>) {
        val castAggregator = aggregator as AccumulatingReadAggregator<K>
        if (v == null) {
            v = castAggregator.v
        } else if (castAggregator.v != null) {
            v?.addAll(castAggregator.v!!)
        }
    }

    override fun aggregate(): AclKeySet? {
        return v
    }
}