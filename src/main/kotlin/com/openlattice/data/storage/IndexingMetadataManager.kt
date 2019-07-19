package com.openlattice.data.storage

import com.openlattice.data.EntityDataKey
import com.openlattice.postgres.DataTables.LAST_INDEX
import com.openlattice.postgres.DataTables.LAST_LINK
import com.openlattice.postgres.PostgresArrays
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresTable.IDS
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.LoggerFactory
import java.time.OffsetDateTime
import java.util.UUID
import java.util.Optional


class IndexingMetadataManager(private val hds: HikariDataSource) {
    companion object {
        private val logger = LoggerFactory.getLogger(IndexingMetadataManager::class.java)
    }

    fun markAsIndexed(entityKeyIdsWithLastWrite: Map<UUID, Map<UUID, OffsetDateTime>>, linking: Boolean): Int {
        return hds.connection.use { connection ->
            val updateSql = updateLastIndexSql(linking)
            connection.prepareStatement(updateSql).use { stmt ->
                entityKeyIdsWithLastWrite.forEach { (entitySetId, entities) ->
                    entities.forEach { (entityId, lastWrite) ->
                        stmt.setObject(1, entitySetId)
                        stmt.setObject(2, entityId)
                        stmt.setObject(3, lastWrite)
                        stmt.addBatch()
                    }
                }

                return stmt.executeBatch().sum()
            }
        }
    }

    fun markEntitySetsAsNeedsToBeIndexed(entitySetIds: Set<UUID>, linking: Boolean): Int {
        return hds.connection.use { connection ->
            val updateSql = markEntitySetsAsNeedsToBeIndexedSql(linking)
            connection.prepareStatement(updateSql).use { stmt ->
                val entitySetIdsArray = PostgresArrays.createUuidArray(connection, entitySetIds)
                stmt.setArray(1, entitySetIdsArray)
                return stmt.executeUpdate()
            }
        }
    }

    fun markLinkingIdsAsNeedToBeIndexed(linkingIds: Set<UUID>): Int {
        hds.connection.use { connection ->
            connection.prepareStatement(markLinkingIdsAsNeedToBeIndexedSql()).use { stmt ->
                val arr = PostgresArrays.createUuidArray(connection, linkingIds)
                stmt.setArray(1, arr)
                return stmt.executeUpdate()
            }
        }
    }

    fun markAsNeedsToBeLinked(entityDataKeys: Set<EntityDataKey>): Int {
        return hds.connection.use { connection ->
            connection.prepareStatement(markAsNeedsToBeLinkedSql(entityDataKeys)).use { stmt ->
                stmt.executeUpdate()
            }
        }
    }
}

/**
 * Arguments of preparable sql in order:
 * 1. entity set id
 * 2. entity key id
 * 3. if linking      -> last_linking_index
 *    if non-linking  -> last_index
 */
fun updateLastIndexSql(linking: Boolean): String {
    val idColumn = if (linking) LINKING_ID.name else ID.name
    val filterLinkingIds = if (linking) " AND ${LINKING_ID.name} IS NOT NULL " else ""
    val entitiesClause = " AND ${ENTITY_SET_ID.name} = ? AND  $idColumn = ? $filterLinkingIds"
    val withClause = buildWithClause(linking, entitiesClause)
    // rather use id than linking_id in linking join
    val joinClause = joinClause(IDS.name, FILTERED_ENTITY_KEY_IDS, entityKeyIdColumnsList)
    val updateColumn = if (linking) LAST_LINK_INDEX.name else LAST_INDEX.name

    return "$withClause UPDATE ${IDS.name} SET $updateColumn = ? " +
            "FROM $FILTERED_ENTITY_KEY_IDS WHERE ($joinClause)"
}

/**
 * Arguments of preparable sql in order:
 * 1. entity set ids (array)
 */
fun markEntitySetsAsNeedsToBeIndexedSql(linking: Boolean): String {
    val entitiesClause = " AND ${ENTITY_SET_ID.name} = ANY(?) "
    val withClause = buildWithClause(linking, entitiesClause)
    // rather use id than linking_id in linking join
    val joinClause = joinClause(IDS.name, FILTERED_ENTITY_KEY_IDS, entityKeyIdColumnsList)
    val updateColumn = if (linking) LAST_LINK_INDEX.name else LAST_INDEX.name

    return "$withClause UPDATE ${IDS.name} SET $updateColumn = '-infinity()' " +
            "FROM $FILTERED_ENTITY_KEY_IDS WHERE ($joinClause) "
}

/**
 * Arguments of preparable sql in order:
 * 1. linking ids (array)
 */
fun markLinkingIdsAsNeedToBeIndexedSql(): String {
    val linkingEntitiesClause = " AND ${LINKING_ID.name} IS NOT NULL AND ${LINKING_ID.name} = ANY(?) "
    val withClause = buildWithClause(true, linkingEntitiesClause)
    // rather use id than linking_id in join
    val joinClause = joinClause(IDS.name, FILTERED_ENTITY_KEY_IDS, entityKeyIdColumnsList)

    return "$withClause UPDATE ${IDS.name} SET ${LAST_LINK_INDEX.name} = '-infinity()' " +
            "FROM $FILTERED_ENTITY_KEY_IDS WHERE ($joinClause) "
}

/**
 * No arguments needed for preparable statement
 */
fun markAsNeedsToBeLinkedSql(entityDataKeys: Set<EntityDataKey>): String {
    val entityKeyIds = entityDataKeys
            .groupBy { it.entitySetId }
            .mapValues { Optional.of(it.value.map { it.entityKeyId }.toSet()) }
    val entitiesClause = buildEntitiesClause(entityKeyIds, true)
    val withClause = buildWithClause(true, entitiesClause)
    // rather use id than linking_id in join
    val joinClause = joinClause(IDS.name, FILTERED_ENTITY_KEY_IDS, entityKeyIdColumnsList)

    return "$withClause UPDATE ${IDS.name} SET ${LAST_LINK.name} = '-infinity()' " +
            "FROM $FILTERED_ENTITY_KEY_IDS WHERE ($joinClause) "
}

private fun joinClause(table1: String, table2: String, joinColumns: List<String>): String {
    return joinColumns.joinToString(" AND ") { column ->
        "$table1.$column = $table2.$column"
    }
}

internal fun buildWithClause(linking: Boolean, entitiesClause: String): String {
    val joinColumns = if (linking) {
        listOf(ENTITY_SET_ID.name, ID_VALUE.name, LINKING_ID.name)
    } else {
        listOf(ENTITY_SET_ID.name, ID_VALUE.name)
    }
    val selectColumns = joinColumns.joinToString(",") { "${IDS.name}.$it AS $it" }

    val queriesSql = "SELECT $selectColumns FROM ${IDS.name} WHERE ${VERSION.name} > 0 $entitiesClause"

    return "WITH $FILTERED_ENTITY_KEY_IDS AS ( $queriesSql ) "
}

