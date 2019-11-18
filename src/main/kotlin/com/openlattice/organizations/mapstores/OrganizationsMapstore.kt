package com.openlattice.organizations.mapstores

import com.dataloom.mappers.ObjectMappers
import com.fasterxml.jackson.module.kotlin.readValue
import com.hazelcast.config.InMemoryFormat
import com.hazelcast.config.MapConfig
import com.hazelcast.config.MapIndexConfig
import com.hazelcast.config.MapStoreConfig
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.mapstores.TestDataFactory
import com.openlattice.organizations.Organization
import com.openlattice.postgres.PostgresColumn.ORGANIZATION
import com.openlattice.postgres.PostgresColumnDefinition
import com.openlattice.postgres.PostgresTable.ORGANIZATIONS
import com.openlattice.postgres.ResultSetAdapters.organizationId
import com.openlattice.postgres.mapstores.AbstractBasePostgresMapstore
import com.zaxxer.hikari.HikariDataSource
import org.springframework.stereotype.Component
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.util.*

const val AUTO_ENROLL = "autoEnroll[any]"

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Component
class OrganizationsMapstore(val hds: HikariDataSource) : AbstractBasePostgresMapstore<UUID, Organization>(
        HazelcastMap.ORGANIZATIONS.name, ORGANIZATIONS, hds
) {
    private val mapper = ObjectMappers.newJsonMapper()
    override fun initValueColumns(): List<PostgresColumnDefinition> {
        return listOf(ORGANIZATION)
    }

    override fun generateTestKey(): UUID {
        return UUID.randomUUID()
    }

    override fun generateTestValue(): Organization {
        return TestDataFactory.organization()
    }

    override fun bind(ps: PreparedStatement, key: UUID, value: Organization) {
        var offset = bind(ps, key)
        ps.setObject(offset++, mapper.writeValueAsString(value))
        ps.setObject(offset++, mapper.writeValueAsString(value))
    }

    override fun mapToKey(rs: ResultSet): UUID {
        return organizationId(rs)
    }

    override fun bind(ps: PreparedStatement, key: UUID, offset: Int): Int {
        ps.setObject(1, key)
        return 1
    }

    override fun mapToValue(rs: ResultSet): Organization {
        return mapper.readValue(rs.getString(ORGANIZATION.name))
    }

    override fun getMapConfig(): MapConfig {
        return super.getMapConfig()
                .addMapIndexConfig(MapIndexConfig(AUTO_ENROLL, false))
                .setInMemoryFormat(InMemoryFormat.OBJECT)
    }

    override fun getMapStoreConfig(): MapStoreConfig {
        return super.getMapStoreConfig()
                .setImplementation(this)
    }
}
