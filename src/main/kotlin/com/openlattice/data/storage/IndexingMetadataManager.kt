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
        hds.connection.use {
            it.prepareStatement(markLinkingIdsAsNeedToBeIndexedSql()).use {
                val arr = PostgresArrays.createUuidArray(it.connection, linkingIds)
                it.setArray(1, arr)
                return it.executeUpdate()
            }
        }
    }

    fun markAsNeedsToBeIndexed(entityKeyIds: Map<UUID, Optional<Set<UUID>>>, linking: Boolean): Int {
        return updateLastIndex(entityKeyIds, linking, OffsetDateTime.MIN)
    }

    private fun updateLastIndex(entityKeyIds: Map<UUID, Optional<Set<UUID>>>, linking: Boolean, dateTime: OffsetDateTime): Int {
        hds.connection.use { connection ->
            val updateSql =
                    if (linking) updateLastLinkIndexSql(entityKeyIds) else updateLastIndexSql(entityKeyIds)
            connection.prepareStatement(updateSql)
                    .use {
                        it.setObject(1, dateTime)
                        return it.executeUpdate()
                    }
        }
    }

    fun markAsNeedsToBeLinked(entityDataKeys: Set<EntityDataKey>): Int {
        return hds.connection.use {
            it.prepareStatement(markAsNeedsToBeLinkedSql(entityDataKeys)).executeUpdate()
        }
    }
}


fun updateLastIndexSql(idsByEntitySetId: Map<UUID, Optional<Set<UUID>>>): String {
    val entitiesClause = buildEntitiesClause(idsByEntitySetId, false)

    return "UPDATE ${IDS.name} SET ${LAST_INDEX.name} = ? " +
            "WHERE TRUE $entitiesClause "
}

fun updateLastLinkIndexSql(linkingIdsByEntitySetId: Map<UUID, Optional<Set<UUID>>>): String {
    val entitiesClause = buildEntitiesClause(linkingIdsByEntitySetId, true)

    return "UPDATE ${IDS.name} SET ${LAST_LINK_INDEX.name} = ? " +
            "WHERE TRUE $entitiesClause"
}

fun markLinkingIdsAsNeedToBeIndexedSql(): String {
    return "UPDATE ${IDS.name} SET ${LAST_LINK_INDEX.name} = '-infinity()' " +
            "WHERE ${LINKING_ID.name} IN (SELECT UNNEST( (?)::uuid[] )) "
}

fun markAsNeedsToBeLinkedSql(entityDataKeys: Set<EntityDataKey>): String {
    val entitiesClause = entityDataKeys.joinToString("OR") {
        "(${ENTITY_SET_ID.name} = '${it.entitySetId}' AND ${ID.name} = '${it.entityKeyId}')"
    }
    return "UPDATE ${IDS.name} SET ${LAST_LINK.name} = '-infinity()' " +
            "WHERE $entitiesClause"
}

