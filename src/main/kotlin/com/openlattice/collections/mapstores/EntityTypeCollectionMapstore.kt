package com.openlattice.collections.mapstores

import com.dataloom.mappers.ObjectMappers
import com.fasterxml.jackson.databind.ObjectMapper
import com.openlattice.edm.collection.EntityTypeCollection
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.mapstores.TestDataFactory
import com.openlattice.postgres.PostgresArrays
import com.openlattice.postgres.PostgresTable
import com.openlattice.postgres.ResultSetAdapters
import com.openlattice.postgres.mapstores.AbstractBasePostgresMapstore
import com.zaxxer.hikari.HikariDataSource
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.util.*

private val mapper: ObjectMapper = ObjectMappers.getJsonMapper()

open class EntityTypeCollectionMapstore(
        hds: HikariDataSource
) : AbstractBasePostgresMapstore<UUID, EntityTypeCollection>
(HazelcastMap.ENTITY_TYPE_COLLECTIONS.name, PostgresTable.ENTITY_TYPE_COLLECTIONS, hds) {

    override fun bind(ps: PreparedStatement, key: UUID, value: EntityTypeCollection) {
        var index = bind(ps, key, 1)

        val fqn = value.type
        val schemas = PostgresArrays.createTextArray(
                ps.connection,
                value.schemas.map { it.fullQualifiedNameAsString })
        val templateString = mapper.writeValueAsString(value.template)

        // Create
        ps.setString(index++, fqn.namespace)
        ps.setString(index++, fqn.name)
        ps.setString(index++, value.title)
        ps.setString(index++, value.description)
        ps.setArray(index++, schemas)
        ps.setString(index++, templateString)

        // Update
        ps.setString(index++, fqn.namespace)
        ps.setString(index++, fqn.name)
        ps.setString(index++, value.title)
        ps.setString(index++, value.description)
        ps.setArray(index++, schemas)
        ps.setString(index++, templateString)
    }

    override fun bind(ps: PreparedStatement, key: UUID, offset: Int): Int {
        var index = offset
        ps.setObject(index++, key)

        return index
    }

    override fun mapToKey(rs: ResultSet?): UUID {
        return ResultSetAdapters.id(rs)
    }

    override fun mapToValue(rs: ResultSet?): EntityTypeCollection {
        return ResultSetAdapters.entityTypeCollection(rs)
    }

    override fun generateTestKey(): UUID {
        return UUID.randomUUID()
    }

    override fun generateTestValue(): EntityTypeCollection {
        return TestDataFactory.entityTypeCollection()
    }
}