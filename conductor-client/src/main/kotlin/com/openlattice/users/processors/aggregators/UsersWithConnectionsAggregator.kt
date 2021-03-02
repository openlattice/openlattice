package com.openlattice.users.processors.aggregators

import com.auth0.json.mgmt.users.User
import com.hazelcast.aggregation.Aggregator

data class UsersWithConnectionsAggregator(
        val connections: Set<String>,
        val users: MutableSet<User>
) : Aggregator<MutableMap.MutableEntry<String, User>, Set<User>> {

    override fun accumulate(input: MutableMap.MutableEntry<String, User>) {
        if (input.value.identities.any { connections.contains(it.connection) }) {
            users.add(input.value)
        }
    }

    override fun combine(aggregator: Aggregator<*, *>) {
        if (aggregator is UsersWithConnectionsAggregator) {
            users.addAll(aggregator.users)
        }
    }

    override fun aggregate(): Set<User> {
        return users
    }
}