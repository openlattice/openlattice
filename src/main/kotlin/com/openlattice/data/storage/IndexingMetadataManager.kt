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

    fun markAsIndexed(entityKeyIds: Map<UUID, Optional<Set<UUID>>>, linking: Boolean): Int {
        return updateLastIndex(entityKeyIds, linking, OffsetDateTime.now())
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

    fun markAsNeedsToBeIndexed(entityKeyIds: Map<UUID, Optional<Set<UUID>>>, linking: Boolean): Int {
        return updateLastIndex(entityKeyIds, linking, OffsetDateTime.MIN)
    }

    private fun updateLastIndex(
            entityKeyIds: Map<UUID, Optional<Set<UUID>>>, linking: Boolean, dateTime: OffsetDateTime
    ): Int {
        hds.connection.use { connection ->
            val updateSql = if (linking) updateLastLinkIndexSql(entityKeyIds) else updateLastIndexSql(entityKeyIds)
            connection.prepareStatement(updateSql).use { stmt ->
                stmt.setObject(1, dateTime)
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


fun updateLastIndexSql(idsByEntitySetId: Map<UUID, Optional<Set<UUID>>>): String {
    val entitiesClause = buildEntitiesClause(idsByEntitySetId, false)
    val withClause = buildWithClause(false, entitiesClause)
    val joinClause = joinClause(IDS.name, FILTERED_ENTITY_KEY_IDS, entityKeyIdColumnsList)

    return "$withClause UPDATE ${IDS.name} SET ${LAST_INDEX.name} = ? " +
            "FROM $FILTERED_ENTITY_KEY_IDS WHERE ($joinClause)"
}

fun updateLastLinkIndexSql(linkingIdsByEntitySetId: Map<UUID, Optional<Set<UUID>>>): String {
    val entitiesClause = buildEntitiesClause(linkingIdsByEntitySetId, true)
    val withClause = buildWithClause(true, entitiesClause)
    // rather use id than linking_id in join
    val joinClause = joinClause(IDS.name, FILTERED_ENTITY_KEY_IDS, entityKeyIdColumnsList)

    return "$withClause UPDATE ${IDS.name} SET ${LAST_LINK_INDEX.name} = ? " +
            "FROM $FILTERED_ENTITY_KEY_IDS WHERE ($joinClause) "
}

fun markLinkingIdsAsNeedToBeIndexedSql(): String {
    val linkingEntitiesClause = " AND ${LINKING_ID.name} IS NOT NULL AND ${LINKING_ID.name} = ANY(?) "
    val withClause = buildWithClause(true, linkingEntitiesClause)
    // rather use id than linking_id in join
    val joinClause = joinClause(IDS.name, FILTERED_ENTITY_KEY_IDS, entityKeyIdColumnsList)

    return "$withClause UPDATE ${IDS.name} SET ${LAST_LINK_INDEX.name} = '-infinity()' " +
            "FROM $FILTERED_ENTITY_KEY_IDS WHERE ($joinClause) "
}

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

