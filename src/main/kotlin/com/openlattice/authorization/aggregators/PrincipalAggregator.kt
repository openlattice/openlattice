package com.openlattice.authorization.aggregators


import com.hazelcast.aggregation.Aggregator
import com.openlattice.authorization.*
import java.util.Map


class PrincipalAggregator(private val principalsMap: MutableMap<AclKey, MutableSet<Principal>>
) : Aggregator<Map.Entry<AceKey, AceValue>, PrincipalAggregator>() {

    override fun accumulate(input: Map.Entry<AceKey, AceValue>) {}

    override fun combine(aggregator: Aggregator<*, *>) {
        if (aggregator is PrincipalAggregator) {
            aggregator.principalsMap.forEach {
                if (principalsMap.containsKey(it.key)) {
                    principalsMap[it.key]!!.addAll(it.value)
                } else {
                    principalsMap[it.key] = it.value
                }
            }
        }
    }

    override fun aggregate(): PrincipalAggregator {
        return this
    }

    fun getResult(): MutableMap<AclKey, MutableSet<Principal>> {
        return principalsMap
    }

    override fun equals(other: Any?): Boolean {
        if (other == null) return false
        if (other !is PrincipalAggregator) return false
        return principalsMap == other.principalsMap
    }

    override fun hashCode(): Int {
        return principalsMap.hashCode()
    }

    override fun toString(): String {
        return "AuthorizationAggregator{principalsMap=$principalsMap}"
    }


}