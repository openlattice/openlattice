package com.openlattice.organizations.mapstores

import com.hazelcast.config.InMemoryFormat
import com.hazelcast.config.MapConfig
import com.hazelcast.config.MapStoreConfig
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.mapstores.TestDataFactory
import com.openlattice.organizations.JdbcConnectionParameters
import com.openlattice.postgres.PostgresTable.WAREHOUSES
import com.openlattice.postgres.PostgresColumn.WAREHOUSE
import com.openlattice.postgres.ResultSetAdapters
import com.openlattice.postgres.mapstores.AbstractBasePostgresMapstore
import com.openlattice.postgres.PostgresColumnDefinition
import com.zaxxer.hikari.HikariDataSource
import org.springframework.stereotype.Component
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.geekbeast.mappers.mappers.ObjectMappers
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.util.*

/**
 * @author Andrew Carter andrew@openlattice.com
 */

@Component
class WarehousesMapstore(
    hds: HikariDataSource
) : AbstractBasePostgresMapstore<UUID, JdbcConnectionParameters>(
    HazelcastMap.WAREHOUSES, WAREHOUSES, hds
) {
    private val mapper: ObjectMapper = ObjectMappers.newJsonMapper()

    override fun initValueColumns(): List<PostgresColumnDefinition> {
        return listOf(WAREHOUSE)
    }

    override fun generateTestKey(): UUID {
        return UUID.randomUUID()
    }

    override fun generateTestValue(): JdbcConnectionParameters {
        return TestDataFactory.warehouse()
    }

    override fun bind(ps: PreparedStatement, key: UUID, value: JdbcConnectionParameters) {

        var index = bind(ps, key, 1)
        val orgJson = mapper.writeValueAsString(value)
        ps.setObject(index++, orgJson)
        ps.setObject(index, orgJson)
    }

    override fun bind(ps: PreparedStatement, key: UUID, offset: Int): Int {
        ps.setObject(offset, key)
        return offset + 1
    }

    override fun mapToKey(rs: ResultSet): UUID {
        return ResultSetAdapters.id(rs)
    }

    override fun mapToValue(rs: ResultSet): JdbcConnectionParameters {
        return mapper.readValue(rs.getString(WAREHOUSE.name))
    }

    override fun getMapConfig(): MapConfig {
        return super.getMapConfig()
            .setInMemoryFormat(InMemoryFormat.OBJECT)
    }

    override fun getMapStoreConfig(): MapStoreConfig {
        return super.getMapStoreConfig()
            .setImplementation(this)
    }
}