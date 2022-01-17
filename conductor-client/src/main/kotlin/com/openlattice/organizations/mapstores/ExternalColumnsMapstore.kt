package com.openlattice.organizations.mapstores

import com.hazelcast.config.InMemoryFormat
import com.hazelcast.config.IndexConfig
import com.hazelcast.config.IndexType
import com.hazelcast.config.MapConfig
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.mapstores.TestDataFactory
import com.openlattice.organization.ExternalColumn
import com.openlattice.postgres.PostgresTable
import com.openlattice.postgres.ResultSetAdapters
import com.geekbeast.postgres.mapstores.AbstractBasePostgresMapstore
import com.zaxxer.hikari.HikariDataSource
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.util.*

const val TABLE_ID_INDEX = "tableId"
const val ORGANIZATION_ID_INDEX = "organizationId"

open class ExternalColumnsMapstore(hds: HikariDataSource) : AbstractBasePostgresMapstore<UUID, ExternalColumn>(
        HazelcastMap.EXTERNAL_COLUMNS, PostgresTable.ORGANIZATION_EXTERNAL_DATABASE_COLUMN, hds
) {

    override fun bind(ps: PreparedStatement, key: UUID, value: ExternalColumn) {
        var index = bind(ps, key, 1)

        //create
        ps.setString(index++, value.name)
        ps.setString(index++, value.title)
        ps.setString(index++, value.description)
        ps.setObject(index++, value.tableId)
        ps.setObject(index++, value.organizationId)
        ps.setString(index++, value.dataType.toString())
        ps.setBoolean(index++, value.primaryKey)
        ps.setInt(index++, value.ordinalPosition)

        //update
        ps.setString(index++, value.name)
        ps.setString(index++, value.title)
        ps.setString(index++, value.description)
        ps.setObject(index++, value.tableId)
        ps.setObject(index++, value.organizationId)
        ps.setString(index++, value.dataType.toString())
        ps.setBoolean(index++, value.primaryKey)
        ps.setInt(index++, value.ordinalPosition)
    }

    override fun bind(ps: PreparedStatement, key: UUID, offset: Int): Int {
        var index = offset
        ps.setObject(index++, key)
        return index
    }

    override fun mapToKey(rs: ResultSet): UUID {
        return ResultSetAdapters.id(rs)
    }

    override fun mapToValue(rs: ResultSet): ExternalColumn {
        return ResultSetAdapters.externalColumn(rs)
    }

    override fun getMapConfig(): MapConfig {
        return super.getMapConfig()
                .addIndexConfig(IndexConfig(IndexType.HASH, TABLE_ID_INDEX))
                .addIndexConfig(IndexConfig(IndexType.HASH, ORGANIZATION_ID_INDEX))
                .setInMemoryFormat(InMemoryFormat.OBJECT)
    }

    override fun generateTestKey(): UUID {
        return UUID.randomUUID()
    }

    override fun generateTestValue(): ExternalColumn {
        return TestDataFactory.externalColumn()
    }
}