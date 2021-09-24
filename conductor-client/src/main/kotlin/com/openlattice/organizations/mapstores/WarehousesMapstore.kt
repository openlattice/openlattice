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
import org.postgresql.util.PGobject
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.SQLException
import java.util.*
import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.ObjectMapper
import com.dataloom.mappers.ObjectMappers

/**
 * @author Andrew Carter andrew@openlattice.com
 */

open class WarehousesMapstore(hds: HikariDataSource) : AbstractBasePostgresMapstore<UUID, JdbcConnectionParameters>(
    HazelcastMap.WAREHOUSES, WAREHOUSES, hds
) {
    private val mapper: ObjectMapper = ObjectMappers.newJsonMapper()

    override fun generateTestKey(): UUID {
        return UUID.randomUUID()
    }

    override fun generateTestValue(): JdbcConnectionParameters {
        return TestDataFactory.warehouse()
    }

    override fun bind(ps: PreparedStatement, key: UUID, value: JdbcConnectionParameters) {

        var index = bind(ps, key, 1)

        val propertyTags = PGobject()
        try {
            propertyTags.setType("jsonb")
            propertyTags.setValue(mapper.writeValueAsString(value.properties))
        } catch (e: JsonProcessingException) {
            throw SQLException("Unable to serialize property tags to JSON.", e)
        }

        val descValue = if( value.description.isEmpty() ) "" else value.description.get()

        // insert
        ps.setString(index++, value._title)
        ps.setString(index++, value.url)
        ps.setString(index++, value.driver)
        ps.setString(index++, value.database)
        ps.setString(index++, value.username)
        ps.setString(index++, value.password)
        ps.setObject(index++, propertyTags)
        ps.setObject(index++, descValue)

        // update
        ps.setString(index++, value._title)
        ps.setString(index++, value.url)
        ps.setString(index++, value.driver)
        ps.setString(index++, value.database)
        ps.setString(index++, value.username)
        ps.setString(index++, value.password)
        ps.setObject(index++, propertyTags)
        ps.setObject(index, descValue)

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