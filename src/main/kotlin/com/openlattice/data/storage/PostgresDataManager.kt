package com.openlattice.data.storage

import com.openlattice.postgres.*
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.LoggerFactory
import java.time.OffsetDateTime
import java.util.UUID
import java.util.Optional


class PostgresDataManager(private val hds: HikariDataSource) {
    companion object {
        private val logger = LoggerFactory.getLogger(PostgresDataManager::class.java)
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
        hds.connection.use {
            val updateSql =
                    if (linking) updateLastLinkIndexSql(entityKeyIds) else updateLastIndexSql(entityKeyIds)
            it.prepareStatement(updateSql)
                    .use {
                        it.setObject(1, dateTime)
                        return it.executeUpdate()
                    }
        }
    }

    fun updateLastIndexSql(idsByEntitySetId: Map<UUID, Optional<kotlin.collections.Set<UUID>>>): String {
        val entitiesClause = buildEntitiesClause(idsByEntitySetId, false)

        return "UPDATE ${PostgresTable.IDS.name} SET ${DataTables.LAST_INDEX.name} = ? " +
                "WHERE TRUE $entitiesClause "
    }

    fun updateLastLinkIndexSql(linkingIdsByEntitySetId: Map<UUID, Optional<kotlin.collections.Set<UUID>>>): String {
        val entitiesClause = buildEntitiesClause(linkingIdsByEntitySetId, true)

        return "UPDATE ${PostgresTable.IDS.name} SET ${PostgresColumn.LAST_LINK_INDEX.name} = ? " +
                "WHERE TRUE $entitiesClause"
    }

    fun markLinkingIdsAsNeedToBeIndexedSql(): String {
        return "UPDATE ${PostgresTable.IDS.name} SET ${PostgresColumn.LAST_LINK_INDEX.name} = '-infinity()' " +
                "WHERE ${PostgresColumn.LINKING_ID.name} IN (SELECT UNNEST( (?)::uuid[] )) "
    }
}
