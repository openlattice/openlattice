package com.openlattice.linking.mapstores

import com.hazelcast.config.InMemoryFormat
import com.hazelcast.config.MapConfig
import com.hazelcast.config.MapIndexConfig
import com.openlattice.data.EntityDataKey
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.linking.EntityKeyPair
import com.openlattice.postgres.PostgresTable
import com.openlattice.postgres.ResultSetAdapters
import com.openlattice.postgres.mapstores.AbstractBasePostgresMapstore
import com.zaxxer.hikari.HikariDataSource
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.util.UUID
import kotlin.random.Random

const val FIRST_ENTITY_INDEX = "__key#first"
const val FIRST_ENTITY_SET_INDEX = "__key#firstEntitySetId"
const val FIRST_ENTITY_KEY_INDEX = "__key#firstEntityKeyId"
const val SECOND_ENTITY_INDEX = "__key#second"
const val SECOND_ENTITY_SET_INDEX = "__key#secondEntitySetId"
const val SECOND_ENTITY_KEY_INDEX = "__key#secondEntityKeyId"
const val FEEDBACK_INDEX = "this"

open class LinkingFeedbackMapstore(
        hds: HikariDataSource
) : AbstractBasePostgresMapstore<EntityKeyPair, Boolean>
(HazelcastMap.LINKING_FEEDBACKS.name, PostgresTable.LINKING_FEEDBACK, hds) {

    override fun bind(ps: PreparedStatement, key: EntityKeyPair, value: Boolean) {
        val offset = bind(ps, key, 1)
        ps.setBoolean(offset, value)

        // UPDATE
        ps.setBoolean(offset + 1, value)
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

    override fun mapToValue(rs: ResultSet?): Boolean {
        return ResultSetAdapters.linked(rs)
    }

    override fun getMapConfig(): MapConfig {
        return super
                .getMapConfig()
                .setInMemoryFormat(InMemoryFormat.OBJECT) // will be queried a lot from realtime linking service
                .addMapIndexConfig(MapIndexConfig(FIRST_ENTITY_INDEX, false))
                .addMapIndexConfig(MapIndexConfig(FIRST_ENTITY_SET_INDEX, false))
                .addMapIndexConfig(MapIndexConfig(FIRST_ENTITY_KEY_INDEX, false))
                .addMapIndexConfig(MapIndexConfig(SECOND_ENTITY_INDEX, false))
                .addMapIndexConfig(MapIndexConfig(SECOND_ENTITY_SET_INDEX, false))
                .addMapIndexConfig(MapIndexConfig(SECOND_ENTITY_KEY_INDEX, false))
                .addMapIndexConfig(MapIndexConfig(FEEDBACK_INDEX, false))
    }

    override fun generateTestKey(): EntityKeyPair {
        return EntityKeyPair(
                EntityDataKey(UUID.randomUUID(), UUID.randomUUID()),
                EntityDataKey(UUID.randomUUID(), UUID.randomUUID()))
    }

    override fun generateTestValue(): Boolean {
        return Random.nextBoolean()
    }
}