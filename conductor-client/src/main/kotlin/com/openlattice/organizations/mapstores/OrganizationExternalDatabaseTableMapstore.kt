package com.openlattice.organizations.mapstores

import com.hazelcast.config.InMemoryFormat
import com.hazelcast.config.IndexConfig
import com.hazelcast.config.IndexType
import com.hazelcast.config.MapConfig
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.mapstores.TestDataFactory
import com.openlattice.organization.OrganizationExternalDatabaseTable
import com.openlattice.postgres.PostgresTable
import com.openlattice.postgres.ResultSetAdapters
import com.openlattice.postgres.mapstores.AbstractBasePostgresMapstore
import com.zaxxer.hikari.HikariDataSource
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.util.*

open class OrganizationExternalDatabaseTableMapstore(
        hds: HikariDataSource
) : AbstractBasePostgresMapstore<UUID, OrganizationExternalDatabaseTable>
    (HazelcastMap.ORGANIZATION_EXTERNAL_DATABASE_TABLE, PostgresTable.ORGANIZATION_EXTERNAL_DATABASE_TABLE, hds) {

    override fun bind(ps: PreparedStatement, key: UUID, value: OrganizationExternalDatabaseTable) {
        var index = bind(ps, key, 1)

        //create
        ps.setString(index++, value.name)
        ps.setInt(index++, value.oid)
        ps.setString(index++, value.title)
        ps.setString(index++, value.description)
        ps.setObject(index++, value.organizationId)

        //update
        ps.setString(index++, value.name)
        ps.setInt(index++, value.oid)
        ps.setString(index++, value.title)
        ps.setString(index++, value.description)
        ps.setObject(index++, value.organizationId)
    }

    override fun bind(ps: PreparedStatement, key: UUID, offset: Int): Int {
        var index = offset
        ps.setObject(index++, key)
        return index
    }

    override fun mapToKey(rs: ResultSet): UUID {
        return ResultSetAdapters.id(rs)
    }

    override fun mapToValue(rs: ResultSet): OrganizationExternalDatabaseTable {
        return ResultSetAdapters.organizationExternalDatabaseTable(rs)
    }

    override fun getMapConfig(): MapConfig {
        return super.getMapConfig()
                .addIndexConfig(IndexConfig(IndexType.HASH, ORGANIZATION_ID_INDEX))
                .setInMemoryFormat(InMemoryFormat.OBJECT)
    }

    override fun generateTestKey(): UUID {
        return UUID.randomUUID()
    }

    override fun generateTestValue(): OrganizationExternalDatabaseTable {
        return TestDataFactory.organizationExternalDatabaseTable()
    }
}