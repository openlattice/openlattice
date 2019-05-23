package com.openlattice.collections.mapstores

import com.hazelcast.config.InMemoryFormat
import com.hazelcast.config.MapConfig
import com.hazelcast.config.MapIndexConfig
import com.openlattice.edm.collection.CollectionTemplateKey
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.postgres.PostgresTable
import com.openlattice.postgres.ResultSetAdapters
import com.openlattice.postgres.mapstores.AbstractBasePostgresMapstore
import com.zaxxer.hikari.HikariDataSource
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.util.*

const val ENTITY_SET_COLLECTION_ID_INDEX = "__key#entitySetCollectionId"
const val ENTITY_SET_ID_INDEX = "this"

open class EntitySetCollectionConfigMapstore(
        val hds: HikariDataSource
) : AbstractBasePostgresMapstore<CollectionTemplateKey, UUID>(
        HazelcastMap.ENTITY_SET_COLLECTION_CONFIG.name, PostgresTable.ENTITY_SET_COLLECTION_CONFIG, hds) {

    override fun bind(ps: PreparedStatement, key: CollectionTemplateKey, value: UUID) {
        var index = bind(ps, key, 1)

        // create
        ps.setObject(index++, value)

        // update
        ps.setObject(index++, value)
    }

    override fun bind(ps: PreparedStatement, key: CollectionTemplateKey, offset: Int): Int {
        var index = offset
        ps.setObject(index++, key.entitySetCollectionId)
        ps.setObject(index++, key.templateTypeId)

        return index
    }

    override fun mapToKey(rs: ResultSet?): CollectionTemplateKey {
        return ResultSetAdapters.collectionTemplateKey(rs)
    }

    override fun mapToValue(rs: ResultSet?): UUID {
        return ResultSetAdapters.entitySetId(rs)
    }

    override fun getMapConfig(): MapConfig {
        return super.getMapConfig()
                .addMapIndexConfig(MapIndexConfig(ENTITY_SET_COLLECTION_ID_INDEX, false))
                .addMapIndexConfig(MapIndexConfig(ENTITY_SET_ID_INDEX, false))
                .setInMemoryFormat(InMemoryFormat.OBJECT)
    }

    override fun generateTestKey(): CollectionTemplateKey {
        return CollectionTemplateKey(UUID.randomUUID(), UUID.randomUUID())
    }

    override fun generateTestValue(): UUID {
        return UUID.randomUUID()
    }
}