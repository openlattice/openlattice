package com.openlattice.organizations.mapstores

import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.mapstores.TestDataFactory
import com.openlattice.organizations.OrganizationDatabase
import com.openlattice.postgres.PostgresTable
import com.openlattice.postgres.ResultSetAdapters
import com.openlattice.postgres.mapstores.AbstractBasePostgresMapstore
import com.zaxxer.hikari.HikariDataSource
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.util.*

open class OrganizationDatabasesMapstore(
        hds: HikariDataSource
) : AbstractBasePostgresMapstore<UUID, OrganizationDatabase>
(HazelcastMap.ORGANIZATION_DATABASES, PostgresTable.ORGANIZATION_DATABASES, hds) {

    override fun bind(ps: PreparedStatement, key: UUID, value: OrganizationDatabase) {
        var index = bind(ps, key, 1)

        //create
        ps.setInt(index++, value.oid)
        ps.setString(index++, value.name)

        //update
        ps.setInt(index++, value.oid)
        ps.setString(index++, value.name)
    }

    override fun bind(ps: PreparedStatement, key: UUID, offset: Int): Int {
        var index = offset
        ps.setObject(index++, key)
        return index
    }

    override fun mapToKey(rs: ResultSet): UUID {
        return ResultSetAdapters.id(rs)
    }

    override fun mapToValue(rs: ResultSet): OrganizationDatabase {
        return ResultSetAdapters.organizationDatabase(rs)
    }

    override fun generateTestKey(): UUID {
        return UUID.randomUUID()
    }

    override fun generateTestValue(): OrganizationDatabase {
        return TestDataFactory.organizationDatabase()
    }
}