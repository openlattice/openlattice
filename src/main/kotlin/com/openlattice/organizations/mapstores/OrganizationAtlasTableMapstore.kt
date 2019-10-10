package com.openlattice.organizations.mapstores

import com.hazelcast.config.InMemoryFormat
import com.hazelcast.config.MapConfig
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.mapstores.TestDataFactory
import com.openlattice.organization.OrganizationAtlasColumn
import com.openlattice.organization.OrganizationAtlasTable
import com.openlattice.postgres.PostgresArrays
import com.openlattice.postgres.PostgresTable
import com.openlattice.postgres.ResultSetAdapters
import com.openlattice.postgres.mapstores.AbstractBasePostgresMapstore
import com.zaxxer.hikari.HikariDataSource
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.util.*

class OrganizationAtlasTableMapstore(
        hds: HikariDataSource
) : AbstractBasePostgresMapstore<UUID, OrganizationAtlasTable>
(HazelcastMap.ORGANIZATION_ATLAS_TABLE.name, PostgresTable.ORGANIZATION_ATLAS_TABLE, hds) {

    override fun bind(ps: PreparedStatement, key: UUID, value: OrganizationAtlasTable) {
        var index = bind(ps, key, 1)

        //create
        ps.setString(index++, value.name)
        ps.setString(index++, value.title)
        ps.setString(index++, value.description)
        ps.setObject(index++, value.organizationId)

        //update
        ps.setString(index++, value.name)
        ps.setString(index++, value.title)
        ps.setString(index++, value.description)
        ps.setObject(index++, value.organizationId)
    }

    override fun bind(ps: PreparedStatement, key: UUID, offset: Int) : Int {
        var index = offset
        ps.setObject(index++, key)
        return index
    }

    override fun mapToKey(rs: ResultSet?): UUID {
        return ResultSetAdapters.id(rs)
    }

    override fun mapToValue(rs: ResultSet?): OrganizationAtlasTable {
        return ResultSetAdapters.organizationAtlasTable(rs)
    }

    override fun getMapConfig(): MapConfig {
        return super.getMapConfig()
                .setInMemoryFormat( InMemoryFormat.OBJECT )
        //TODO ask about the other fields
    }

    override fun generateTestKey(): UUID {
        return UUID.randomUUID()
    }

    override fun generateTestValue(): OrganizationAtlasTable {
        return TestDataFactory.organizationAtlasTable()
    }
}