package com.openlattice.data.storage

import com.openlattice.data.EntityDataKey
import com.openlattice.postgres.DataTables.LAST_INDEX
import com.openlattice.postgres.DataTables.LAST_LINK
import com.openlattice.postgres.PostgresColumn.ENTITY_SET_ID
import com.openlattice.postgres.PostgresColumn.ID
import com.openlattice.postgres.PostgresColumn.LAST_LINK_INDEX
import com.openlattice.postgres.PostgresColumn.LINKING_ID
import com.openlattice.postgres.PostgresColumn.PARTITION
import com.openlattice.postgres.PostgresColumn.VERSION
import com.openlattice.postgres.PostgresTable.IDS
import com.zaxxer.hikari.HikariDataSource
import java.sql.PreparedStatement
import java.time.OffsetDateTime
import java.util.*


class IndexingMetadataManager(
        private val dataSourceResolver: DataSourceResolver
) {

    /**
     * Marks entities as indexed by setting last_index = last_write.
     * @param entityKeyIdsWithLastWrite Map of (normal) entity_set_id to id to last_write.
     */
    fun markAsIndexed(
            entityKeyIdsWithLastWrite: Map<UUID, Map<UUID, OffsetDateTime>> // entity_set_id -> id -> last_write
    ): Int {
        return getByDataSource(dataSourceResolver, entityKeyIdsWithLastWrite) { it }
                .map { (dataSourceName, entityKeyIdsWithLastWriteForDataSource) ->
                    val hds = dataSourceResolver.getDataSource(dataSourceName)
                    hds.connection.use { connection ->
                        val ps = connection.prepareStatement(updateLastIndexSql)
                        entityKeyIdsWithLastWriteForDataSource.map { (entitySetId, idsAndExpirationsMap) ->
                            prepareIndexQuery(
                                    ps,
                                    entitySetId,
                                    idsAndExpirationsMap
                            )
                            ps.executeBatch().sum()
                        }.sum()
                    }

                }.sum()
    }

    /**
     * TODO: I have no idea if this still works.
     * Marks linking entities as indexed by setting last_index = last_write.
     * @param linkingIdsWithLastWrite Map of (normal) entity_set_id to origin id to linking_id to last_write.
     * @return The number of rows affected by this update: the number of normal entities associated to the provided
     * linking ids.
     */
    fun markLinkingEntitiesAsIndexed(
            linkingIdsWithLastWrite: Map<UUID, Map<UUID, Map<UUID, OffsetDateTime>>>
    ): Int {
        return getByDataSource(dataSourceResolver, linkingIdsWithLastWrite) { it }
                .map { (dataSourceName, linkingIdsWithLastWriteForDataSource) ->
                    val hds = dataSourceResolver.getDataSource(dataSourceName)
                    hds.connection.use { connection ->
                        val ps = connection.prepareStatement(updateLastLinkingIndexSql)
                        linkingIdsWithLastWriteForDataSource.map { (entitySetId, idsAndExpiration) ->

                            val mergedLinkingIdsWithLastWrite = idsAndExpiration.values.fold(
                                    mutableMapOf<UUID, OffsetDateTime>()
                            ) { acc, map ->
                                acc.putAll(map)
                                acc
                            }

                            prepareIndexQuery(
                                    ps,
                                    entitySetId,
                                    mergedLinkingIdsWithLastWrite
                            )
                            ps.executeBatch().sum()
                        }.sum()
                    }
                }.sum()

    }

    private fun prepareIndexQuery(
            stmt: PreparedStatement,
            entitySetId: UUID,
            idsWithLastWrite: Map<UUID, OffsetDateTime>
    ) {
        idsWithLastWrite
                .forEach { (id, lastWrite) ->
                    stmt.setObject(1, lastWrite)
                    stmt.setObject(2, entitySetId)
                    stmt.setObject(3, id)
                    stmt.addBatch()
                }
    }

    /**
     * Sets the last_index/last_link_index of provided entities to current datetime. Used when un-indexing entities
     * after deletion.
     * @param entityKeyIds Map of (normal) entity set ids to entity key ids.
     */
    fun markAsUnIndexed(entityKeyIds: Map<UUID, Set<UUID>>): Int {

        return getByDataSource(dataSourceResolver, entityKeyIds) { it }
                .map { (dataSourceName, entityKeyIdsForDataSource) ->
                    val hds = dataSourceResolver.getDataSource(dataSourceName)
                    hds.connection.use { connection ->
                        val ps = connection.prepareStatement(markLastIndexSql)
                        entityKeyIdsForDataSource.map { (entitySetId, entityKeyIdSet) ->
                            entityKeyIdSet.forEach { id ->
                                ps.setObject(1, entitySetId)
                                ps.setObject(2, id)
                                ps.addBatch()
                            }
                            ps.executeBatch().sum()
                        }.sum()
                    }
                }.sum()
    }

    /**
     * Sets the last_write of provided entity set to current datetime. Used when un-indexing entities after entity set
     * data deletion.
     * @param entitySetId The id of the (normal) entity set id.
     */
    fun markAsUnIndexed(entitySetId: UUID): Int {
        val hds = dataSourceResolver.resolve(entitySetId)
        return hds.connection.use { connection ->
            val ps = connection.prepareStatement(markEntitySetLastIndexSql)
            ps.setObject(1, entitySetId)
            ps.executeUpdate()
        }
    }


    fun markEntitySetsAsNeedsToBeIndexed(entitySetIds: Set<UUID>, linking: Boolean): Int {
        val query = markEntitySetsAsNeedsToBeIndexedSql(linking)

        return getByDataSource(dataSourceResolver, entitySetIds) { it }
                .map { (dataSourceName, entitySetIdsForDataSource) ->
                    val hds = dataSourceResolver.getDataSource(dataSourceName)
                    hds.connection.use { connection ->
                        val ps = connection.prepareStatement(query)
                        entitySetIdsForDataSource.sumOf { entitySetId ->
                            ps.setObject(1, entitySetId)
                            ps.executeUpdate()
                        }
                    }
                }.sum()
    }

    fun markAsNeedsToBeLinked(normalEntityDataKeys: Set<EntityDataKey>): Int {
        val normalEntityKeys = normalEntityDataKeys
                .groupBy { it.entitySetId }
                .mapValues { it.value.map { edk -> edk.entityKeyId } }

        return getByDataSource(dataSourceResolver, normalEntityKeys) { it }
                .map { (dataSourceName, normalEntityDataKeyIdsForDataSource) ->
                    val hds = dataSourceResolver.getDataSource(dataSourceName)
                    hds.connection.use { connection ->
                        val ps = connection.prepareStatement(markAsNeedsToBeLinkedSql)
                        normalEntityDataKeyIdsForDataSource.map { (entitySetId, ids) ->
                            ids.forEach { id ->
                                ps.setObject(1, entitySetId)
                                ps.setObject(2, id)
                                ps.addBatch()
                            }
                            ps.executeBatch().sum()
                        }.sum()
                    }
                }.sum()
    }
}
// @formatter:off

/**
 * 1. entity set id
 * 2. entity key id
 */
private val entityKeyIdInEntitySet =
        " ${ENTITY_SET_ID.name} = ? " +
                "AND ${ID.name} = ? "

/**
 * 1. entity set id
 * 2. linking id
 */
private val linkingIdInEntitySet =
        " ${ENTITY_SET_ID.name} = ? " +
                "AND ${LINKING_ID.name} = ? "

/**
 * 1. entity set id
 */
private val entitySetIdClause =
        " ${ENTITY_SET_ID.name} = ? "


/**
 * Arguments of preparable sql in order:
 * 1. last index
 * 2. entity set id
 * 3. entity key id
 */
private val updateLastIndexSql = "UPDATE ${IDS.name} SET ${LAST_INDEX.name} = ? WHERE $entityKeyIdInEntitySet"

/**
 * Arguments of preparable sql in order:
 * 1. last linking index
 * 2. entity set id
 * 3. linking ids (uuid array)
 */
private val updateLastLinkingIndexSql =
        "UPDATE ${IDS.name} SET ${LAST_LINK_INDEX.name} = ? WHERE $linkingIdInEntitySet"

/**
 * Arguments of preparable sql in order:
 * 1. entity set id
 * 2. entity key ids (uuid array)
 */
private val markLastIndexSql = "UPDATE ${IDS.name} SET ${LAST_INDEX.name} = 'now()' WHERE $entityKeyIdInEntitySet"

/**
 * Arguments of preparable sql in order:
 * 1. entity set id
 */
private val markEntitySetLastIndexSql = "UPDATE ${IDS.name} SET ${LAST_INDEX.name} = 'now()' WHERE $entitySetIdClause"



/**
 * Arguments of preparable sql in order:
 * 1. entity set id
 */
fun markEntitySetsAsNeedsToBeIndexedSql(linking: Boolean): String {
    val updateColumn = if (linking) LAST_LINK_INDEX.name else LAST_INDEX.name

    return "UPDATE ${IDS.name} SET $updateColumn = '-infinity()' " +
            "WHERE ${ENTITY_SET_ID.name} = ? "
}

/**
 * Arguments of preparable sql in order:
 * 1. entity set id
 * 2. normal entity key ids (uuid array)
 */
private val markAsNeedsToBeLinkedSql =
        "UPDATE ${IDS.name} SET ${LAST_LINK.name} = '-infinity()' WHERE ${VERSION.name} > 0 AND $entityKeyIdInEntitySet"

// @formatter:on
