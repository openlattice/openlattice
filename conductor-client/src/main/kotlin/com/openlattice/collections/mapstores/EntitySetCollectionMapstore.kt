package com.openlattice.collections.mapstores

import com.hazelcast.config.InMemoryFormat
import com.hazelcast.config.IndexConfig
import com.hazelcast.config.IndexType
import com.hazelcast.config.MapConfig
import com.openlattice.collections.EntitySetCollection
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.mapstores.TestDataFactory
import com.geekbeast.postgres.PostgresArrays
import com.openlattice.postgres.PostgresTable
import com.openlattice.postgres.ResultSetAdapters
import com.geekbeast.postgres.mapstores.AbstractBasePostgresMapstore
import com.zaxxer.hikari.HikariDataSource
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.util.*

open class EntitySetCollectionMapstore(
        hds: HikariDataSource
) : AbstractBasePostgresMapstore<UUID, EntitySetCollection>
    (HazelcastMap.ENTITY_SET_COLLECTIONS, PostgresTable.ENTITY_SET_COLLECTIONS, hds) {

    companion object {
        const val ID_INDEX = "__key"
        const val ENTITY_TYPE_COLLECTION_ID_INDEX = "entityTypeCollectionId"
        const val ORGANIZATION_ID_INDEX = "organizationId"
    }

    override fun bind(ps: PreparedStatement, key: UUID, value: EntitySetCollection) {
        var index = bind(ps, key, 1)

        val contacts = PostgresArrays.createTextArray(ps.connection, value.contacts)

        // Create
        ps.setString(index++, value.name)
        ps.setString(index++, value.title)
        ps.setString(index++, value.description)
        ps.setArray(index++, contacts)
        ps.setObject(index++, value.entityTypeCollectionId)
        ps.setObject(index++, value.organizationId)

        // Update
        ps.setString(index++, value.name)
        ps.setString(index++, value.title)
        ps.setString(index++, value.description)
        ps.setArray(index++, contacts)
        ps.setObject(index++, value.entityTypeCollectionId)
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

    override fun mapToValue(rs: ResultSet): EntitySetCollection {
        return ResultSetAdapters.entitySetCollection(rs)
    }

    override fun getMapConfig(): MapConfig {
        return super.getMapConfig()
                .addIndexConfig(IndexConfig(IndexType.HASH, ID_INDEX))
                .addIndexConfig(IndexConfig(IndexType.HASH, ENTITY_TYPE_COLLECTION_ID_INDEX))
                .addIndexConfig(IndexConfig(IndexType.HASH, ORGANIZATION_ID_INDEX))
                .setInMemoryFormat(InMemoryFormat.OBJECT)
    }

    override fun generateTestKey(): UUID {
        return UUID.randomUUID()
    }

    override fun generateTestValue(): EntitySetCollection {
        return TestDataFactory.entitySetCollection()
    }
}