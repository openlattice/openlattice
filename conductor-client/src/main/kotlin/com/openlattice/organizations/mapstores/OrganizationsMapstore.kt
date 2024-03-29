package com.openlattice.organizations.mapstores

import com.geekbeast.mappers.mappers.ObjectMappers
import com.fasterxml.jackson.module.kotlin.readValue
import com.hazelcast.config.InMemoryFormat
import com.hazelcast.config.IndexConfig
import com.hazelcast.config.IndexType
import com.hazelcast.config.MapConfig
import com.hazelcast.config.MapStoreConfig
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.mapstores.TestDataFactory
import com.openlattice.organizations.Organization
import com.openlattice.postgres.PostgresColumn.ORGANIZATION
import com.geekbeast.postgres.PostgresColumnDefinition
import com.openlattice.postgres.PostgresTable.ORGANIZATIONS
import com.openlattice.postgres.ResultSetAdapters.id
import com.geekbeast.postgres.mapstores.AbstractBasePostgresMapstore
import com.zaxxer.hikari.HikariDataSource
import org.springframework.stereotype.Component
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.util.*

const val CONNECTIONS_INDEX = "connections[any]"
const val MEMBERS_INDEX = "members[any]"
const val DOMAINS_INDEX = "emailDomains[any]"

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Component
class OrganizationsMapstore(
        hds: HikariDataSource
) : AbstractBasePostgresMapstore<UUID, Organization>(
        HazelcastMap.ORGANIZATIONS, ORGANIZATIONS, hds
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
        val offset = bind(ps, key, 1)
        val orgJson = mapper.writeValueAsString(value)
        ps.setObject(offset, orgJson)
    }

    override fun mapToKey(rs: ResultSet): UUID {
        return id(rs)
    }

    override fun bind(ps: PreparedStatement, key: UUID, offset: Int): Int {
        ps.setObject(offset, key)
        return offset + 1
    }

    override fun mapToValue(rs: ResultSet): Organization {
        return mapper.readValue(rs.getString(ORGANIZATION.name))
    }

    override fun getMapConfig(): MapConfig {
        return super.getMapConfig()
                .addIndexConfig(IndexConfig(IndexType.HASH, CONNECTIONS_INDEX))
                .addIndexConfig(IndexConfig(IndexType.HASH, MEMBERS_INDEX))
                .addIndexConfig(IndexConfig(IndexType.HASH, DOMAINS_INDEX))
                .setInMemoryFormat(InMemoryFormat.OBJECT)
    }

    override fun getMapStoreConfig(): MapStoreConfig {
        return super.getMapStoreConfig()
                .setImplementation(this)
    }
}

