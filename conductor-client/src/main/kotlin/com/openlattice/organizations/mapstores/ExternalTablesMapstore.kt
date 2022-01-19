package com.openlattice.organizations.mapstores

import com.hazelcast.config.InMemoryFormat
import com.hazelcast.config.IndexConfig
import com.hazelcast.config.IndexType
import com.hazelcast.config.MapConfig
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.mapstores.TestDataFactory
import com.openlattice.organization.ExternalTable
import com.openlattice.postgres.PostgresTable
import com.openlattice.postgres.ResultSetAdapters
import com.geekbeast.postgres.mapstores.AbstractBasePostgresMapstore
import com.zaxxer.hikari.HikariDataSource
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.util.*

open class ExternalTablesMapstore(hds: HikariDataSource) : AbstractBasePostgresMapstore<UUID, ExternalTable>(
        HazelcastMap.EXTERNAL_TABLES, PostgresTable.ORGANIZATION_EXTERNAL_DATABASE_TABLE, hds
) {

    companion object {
        const val ID_INDEX = "id"
        const val ORGANIZATION_ID_INDEX = "organizationId"
        const val NAME_INDEX = "name"
        const val SCHEMA_INDEX = "schema"
    }

    override fun bind(ps: PreparedStatement, key: UUID, value: ExternalTable) {
        var index = bind(ps, key, 1)

        //create
        ps.setString(index++, value.name)
        ps.setLong(index++, value.oid)
        ps.setString(index++, value.title)
        ps.setString(index++, value.description)
        ps.setObject(index++, value.organizationId)
        ps.setString(index++, value.schema)

        //update
        ps.setString(index++, value.name)
        ps.setLong(index++, value.oid)
        ps.setString(index++, value.title)
        ps.setString(index++, value.description)
        ps.setObject(index++, value.organizationId)
        ps.setString(index++, value.schema)

    }

    override fun bind(ps: PreparedStatement, key: UUID, offset: Int): Int {
        var index = offset
        ps.setObject(index++, key)
        return index
    }

    override fun mapToKey(rs: ResultSet): UUID {
        return ResultSetAdapters.id(rs)
    }

    override fun mapToValue(rs: ResultSet): ExternalTable {
        return ResultSetAdapters.externalTable(rs)
    }

    override fun getMapConfig(): MapConfig {
        return super.getMapConfig()
                .addIndexConfig(IndexConfig(IndexType.HASH, ORGANIZATION_ID_INDEX))
                .addIndexConfig(IndexConfig(IndexType.HASH, ID_INDEX))
                .addIndexConfig(IndexConfig(IndexType.HASH, NAME_INDEX))
                .addIndexConfig(IndexConfig(IndexType.HASH, SCHEMA_INDEX))
                .setInMemoryFormat(InMemoryFormat.OBJECT)
    }

    override fun generateTestKey(): UUID {
        return UUID.randomUUID()
    }

    override fun generateTestValue(): ExternalTable {
        return TestDataFactory.externalTable()
    }
}
