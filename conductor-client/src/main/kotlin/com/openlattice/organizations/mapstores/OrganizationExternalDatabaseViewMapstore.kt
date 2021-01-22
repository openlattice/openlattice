package com.openlattice.organizations.mapstores

import com.hazelcast.config.InMemoryFormat
import com.hazelcast.config.IndexConfig
import com.hazelcast.config.IndexType
import com.hazelcast.config.MapConfig
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.mapstores.TestDataFactory
import com.openlattice.organization.OrganizationExternalDatabaseSchema
import com.openlattice.organization.OrganizationExternalDatabaseView
import com.openlattice.postgres.PostgresTable
import com.openlattice.postgres.ResultSetAdapters
import com.openlattice.postgres.mapstores.AbstractBasePostgresMapstore
import com.zaxxer.hikari.HikariDataSource
import org.springframework.stereotype.Component
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.util.*

@Component
class OrganizationExternalDatabaseViewMapstore(
        hds: HikariDataSource
) : AbstractBasePostgresMapstore<UUID, OrganizationExternalDatabaseView>
    (HazelcastMap.ORGANIZATION_EXTERNAL_DATABASE_VIEW, PostgresTable.ORGANIZATION_EXTERNAL_DATABASE_VIEW, hds) {

    override fun bind(ps: PreparedStatement, key: UUID, value: OrganizationExternalDatabaseView) {
        var index = bind(ps, key, 1)

        //create
        ps.setString(index++, value.name)
        ps.setString(index++, value.title)
        ps.setString(index++, value.description)
        ps.setObject(index++, value.organizationId)
        ps.setObject(index++, value.dataSourceId)
        ps.setString(index++, value.externalId)

        //update
        ps.setString(index++, value.name)
        ps.setString(index++, value.title)
        ps.setString(index++, value.description)
        ps.setObject(index++, value.organizationId)
        ps.setObject(index++, value.dataSourceId)
        ps.setString(index++, value.externalId)
    }

    override fun bind(ps: PreparedStatement, key: UUID, offset: Int): Int {
        var index = offset
        ps.setObject(index++, key)
        return index
    }

    override fun mapToKey(rs: ResultSet): UUID {
        return ResultSetAdapters.id(rs)
    }

    override fun mapToValue(rs: ResultSet): OrganizationExternalDatabaseView {
        return ResultSetAdapters.organizationExternalDatabaseView(rs)
    }

    override fun getMapConfig(): MapConfig {
        return super.getMapConfig()
                .addIndexConfig(IndexConfig(IndexType.HASH, ORGANIZATION_ID_INDEX))
                .addIndexConfig(IndexConfig(IndexType.HASH, DATA_SOURCE_ID_INDEX))
                .setInMemoryFormat(InMemoryFormat.OBJECT)
    }

    override fun generateTestKey(): UUID {
        return UUID.randomUUID()
    }

    override fun generateTestValue(): OrganizationExternalDatabaseView {
        return TestDataFactory.organizationExternalDatabaseView()
    }
}