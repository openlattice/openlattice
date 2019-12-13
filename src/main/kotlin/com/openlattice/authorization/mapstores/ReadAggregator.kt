package com.openlattice.authorization.mapstores

import com.hazelcast.aggregation.Aggregator
import java.util.Map

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class ReadAggregator<K,V> : Aggregator<MutableMap.MutableEntry<K, V>, V>() {
    private var v : V? = null
    override fun accumulate(input: MutableMap.MutableEntry<K, V>) {
        v = input.value
    }

    override fun combine(aggregator: Aggregator<*, *>) {
        val castAggregator = aggregator as ReadAggregator<K,V>
        v = castAggregator.v
    }

    override fun aggregate(): V? {
        return v
    }
}