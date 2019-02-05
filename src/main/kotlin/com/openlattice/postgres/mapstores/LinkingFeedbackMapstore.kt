package com.openlattice.postgres.mapstores

import com.openlattice.data.EntityDataKey
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.linking.EntityKeyPair
import com.openlattice.linking.EntityLinkingFeedback
import com.openlattice.postgres.PostgresTable
import com.openlattice.postgres.ResultSetAdapters
import com.zaxxer.hikari.HikariDataSource
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.util.UUID
import kotlin.random.Random

open class LinkingFeedbackMapstore(
        hds: HikariDataSource
) : AbstractBasePostgresMapstore<EntityKeyPair, EntityLinkingFeedback>
(HazelcastMap.LINKING_FEEDBACKS.name, PostgresTable.LINKING_FEEDBACK, hds) {

    override fun bind(ps: PreparedStatement, key: EntityKeyPair, value: EntityLinkingFeedback) {
        val offset = bind(ps, key, 1)
        ps.setBoolean(offset, value.linked)

        // UPDATE
        ps.setBoolean(offset + 1, value.linked)
    }

    override fun bind(ps: PreparedStatement, key: EntityKeyPair, offset: Int): Int {
        ps.setObject(offset, key.first.entitySetId)
        ps.setObject(offset + 1, key.first.entityKeyId)
        ps.setObject(offset + 2, key.second.entitySetId)
        ps.setObject(offset + 3, key.second.entityKeyId)

        return offset + 4
    }

    override fun mapToKey(rs: ResultSet?): EntityKeyPair {
        return ResultSetAdapters.entityKeyPair(rs)
    }

    override fun mapToValue(rs: ResultSet?): EntityLinkingFeedback {
        return ResultSetAdapters.entityLinkingFeedback(rs)
    }

    // Since value contains key, it needs to be the same when testing
    private lateinit var testKey: EntityKeyPair

    override fun generateTestKey(): EntityKeyPair {
        testKey = EntityKeyPair(
                EntityDataKey(UUID.randomUUID(), UUID.randomUUID()),
                EntityDataKey(UUID.randomUUID(), UUID.randomUUID()))
        return testKey
    }

    override fun generateTestValue(): EntityLinkingFeedback {
        return EntityLinkingFeedback(
                testKey,
                Random.nextBoolean())
    }
}