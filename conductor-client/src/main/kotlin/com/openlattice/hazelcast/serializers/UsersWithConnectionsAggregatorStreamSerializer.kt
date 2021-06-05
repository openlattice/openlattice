package com.openlattice.hazelcast.serializers

import com.auth0.json.mgmt.users.User
import com.google.common.collect.Sets
import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializers
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.mapstores.TestDataFactory
import com.openlattice.users.processors.aggregators.UsersWithConnectionsAggregator
import org.springframework.stereotype.Component

@Component
class UsersWithConnectionsAggregatorStreamSerializer : TestableSelfRegisteringStreamSerializer<UsersWithConnectionsAggregator> {
    override fun generateTestValue(): UsersWithConnectionsAggregator {
        return UsersWithConnectionsAggregator(
                setOf(TestDataFactory.randomAlphanumeric(10), TestDataFactory.randomAlphanumeric(10)),
                mutableSetOf()
        )
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.USERS_WITH_CONNECTIONS_AGGREGATOR.ordinal
    }

    override fun getClazz(): Class<out UsersWithConnectionsAggregator> {
        return UsersWithConnectionsAggregator::class.java
    }

    override fun write(out: ObjectDataOutput, `object`: UsersWithConnectionsAggregator) {
        SetStreamSerializers.fastStringSetSerialize(out, `object`.connections)
        out.writeInt(`object`.users.size)
        `object`.users.forEach { Auth0UserStreamSerializer.serialize(out, it) }
    }

    override fun read(`in`: ObjectDataInput): UsersWithConnectionsAggregator {
        val connections = SetStreamSerializers.fastStringSetDeserialize(`in`)
        val users = (0 until `in`.readInt()).map { Auth0UserStreamSerializer.deserialize(`in`) }.toMutableSet()

        return UsersWithConnectionsAggregator(connections, users)
    }
}