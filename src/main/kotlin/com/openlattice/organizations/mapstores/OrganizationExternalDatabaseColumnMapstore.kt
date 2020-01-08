package com.openlattice.organizations.mapstores

import com.hazelcast.config.InMemoryFormat
import com.hazelcast.config.MapConfig
import com.hazelcast.config.MapIndexConfig
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.mapstores.TestDataFactory
import com.openlattice.organization.OrganizationExternalDatabaseColumn
import com.openlattice.postgres.PostgresTable
import com.openlattice.postgres.ResultSetAdapters
import com.openlattice.postgres.mapstores.AbstractBasePostgresMapstore
import com.zaxxer.hikari.HikariDataSource
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.util.*

const val TABLE_ID_INDEX = "tableId"
const val ORGANIZATION_ID_INDEX = "organizationId"

open class OrganizationExternalDatabaseColumnMapstore(
        hds: HikariDataSource
) : AbstractBasePostgresMapstore<UUID, OrganizationExternalDatabaseColumn>
(HazelcastMap.ORGANIZATION_EXTERNAL_DATABASE_COLUMN, PostgresTable.ORGANIZATION_EXTERNAL_DATABASE_COLUMN, hds) {

    override fun bind(ps: PreparedStatement, key: UUID, value: OrganizationExternalDatabaseColumn) {
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

    override fun bind(ps: PreparedStatement, key: UUID, offset: Int) : Int {
        var index = offset
        ps.setObject(index++, key)
        return index
    }

    override fun mapToKey(rs: ResultSet?): UUID {
        return ResultSetAdapters.id(rs)
    }

    override fun mapToValue(rs: ResultSet?): OrganizationExternalDatabaseColumn {
        return ResultSetAdapters.organizationExternalDatabaseColumn(rs)
    }

    override fun getMapConfig(): MapConfig {
        return super.getMapConfig()
                .addMapIndexConfig(MapIndexConfig(TABLE_ID_INDEX, false))
                .addMapIndexConfig(MapIndexConfig(ORGANIZATION_ID_INDEX, false))
                .setInMemoryFormat( InMemoryFormat.OBJECT )
    }

    override fun generateTestKey(): UUID {
        return UUID.randomUUID()
    }

    override fun generateTestValue(): OrganizationExternalDatabaseColumn {
        return TestDataFactory.organizationExternalDatabaseColumn()
    }
}