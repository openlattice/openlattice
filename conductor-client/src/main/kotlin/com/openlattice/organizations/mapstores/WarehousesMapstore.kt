package com.openlattice.organizations.mapstores

import com.hazelcast.config.InMemoryFormat
import com.hazelcast.config.MapConfig
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.mapstores.TestDataFactory
import com.openlattice.organizations.JdbcConnectionParameters
import com.openlattice.postgres.PostgresTable.WAREHOUSES
import com.openlattice.postgres.ResultSetAdapters
import com.openlattice.postgres.ResultSetAdapters.id
import com.openlattice.postgres.mapstores.AbstractBasePostgresMapstore
import com.zaxxer.hikari.HikariDataSource
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.util.Properties
import java.util.UUID

/**
 * @author Andrew Carter andrew@openlattice.com
 */

open class WarehousesMapstore(hds: HikariDataSource) : AbstractBasePostgresMapstore<UUID, JdbcConnectionParameters>(
    HazelcastMap.WAREHOUSES, WAREHOUSES, hds
) {
    override fun generateTestKey(): UUID {
        return UUID.randomUUID()
    }

    override fun generateTestValue(): JdbcConnectionParameters {
        return TestDataFactory.warehouse()
    }

    override fun bind(ps: PreparedStatement, key: UUID, value: JdbcConnectionParameters) {
        val props = if (value.properties != null) value.properties else Properties()

        var index = bind(ps, key, 1)

        logger.info("Insert value: {}", value)
        logger.info("Insert ps: {}", ps)

        // insert
        ps.setString(index++, value._title)
        ps.setString(index++, value.url)
        ps.setString(index++, value.driver)
        ps.setString(index++, value.database)
        ps.setString(index++, value.username)
        ps.setString(index++, value.password)
        ps.setObject(index++, props)
        ps.setObject(index++, value.description)

        // update
        ps.setString(index++, value._title)
        ps.setString(index++, value.url)
        ps.setString(index++, value.driver)
        ps.setString(index++, value.database)
        ps.setString(index++, value.username)
        ps.setString(index++, value.password)
        ps.setObject(index++, props)
        ps.setObject(index, value.description)

        logger.info("Complete ps: {}", ps)
    }

    override fun bind(ps: PreparedStatement, key: UUID, offset: Int): Int {
        ps.setObject(offset, key)
        return offset + 1
    }

    override fun mapToKey(rs: ResultSet): UUID {
        return ResultSetAdapters.id(rs)
    }

    override fun mapToValue(rs: ResultSet): JdbcConnectionParameters {
        return ResultSetAdapters.warehouses(rs)
    }

    override fun getMapConfig(): MapConfig {
        return super.getMapConfig()
            .setInMemoryFormat(InMemoryFormat.OBJECT)
    }
}