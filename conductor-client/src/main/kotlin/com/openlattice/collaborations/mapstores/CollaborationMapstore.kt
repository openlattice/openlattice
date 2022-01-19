package com.openlattice.collaborations.mapstores

import com.hazelcast.config.InMemoryFormat
import com.hazelcast.config.IndexConfig
import com.hazelcast.config.IndexType
import com.hazelcast.config.MapConfig
import com.openlattice.collaborations.Collaboration
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.mapstores.TestDataFactory
import com.geekbeast.postgres.PostgresArrays
import com.openlattice.postgres.PostgresTable.COLLABORATIONS
import com.openlattice.postgres.ResultSetAdapters
import com.geekbeast.postgres.mapstores.AbstractBasePostgresMapstore
import com.zaxxer.hikari.HikariDataSource
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.util.*

open class CollaborationMapstore(hds: HikariDataSource) : AbstractBasePostgresMapstore<UUID, Collaboration>(
        HazelcastMap.COLLABORATIONS, COLLABORATIONS, hds
) {

    companion object {
        const val ORGANIZATION_ID_IDX = "organizationIds[any]"
    }

    override fun generateTestKey(): UUID {
        return UUID.randomUUID()
    }

    override fun generateTestValue(): Collaboration {
        return TestDataFactory.collaboration()
    }

    override fun bind(ps: PreparedStatement, key: UUID, value: Collaboration) {
        val orgsArray = PostgresArrays.createUuidArray(ps.connection, value.organizationIds)

        var index = bind(ps, key)

        // insert
        ps.setString(index++, value.name)
        ps.setString(index++, value.title)
        ps.setString(index++, value.description)
        ps.setArray(index++, orgsArray)

        // update
        ps.setString(index++, value.name)
        ps.setString(index++, value.title)
        ps.setString(index++, value.description)
        ps.setArray(index, orgsArray)
    }

    override fun getMapConfig(): MapConfig {
        return super.getMapConfig()
                .addIndexConfig(IndexConfig(IndexType.HASH, ORGANIZATION_ID_IDX))
                .setInMemoryFormat(InMemoryFormat.OBJECT)
    }

    override fun bind(ps: PreparedStatement, key: UUID, offset: Int): Int {
        ps.setObject(offset, key)
        return offset + 1
    }

    override fun mapToKey(rs: ResultSet): UUID {
        return ResultSetAdapters.id(rs)
    }

    override fun mapToValue(rs: ResultSet): Collaboration {
        return ResultSetAdapters.collaboration(rs)
    }
}