package com.openlattice.data.storage

import com.openlattice.data.Entity
import com.openlattice.edm.type.PropertyType
import com.openlattice.postgres.*
import com.openlattice.postgres.streams.PostgresIterable
import com.openlattice.postgres.streams.StatementHolder
import com.zaxxer.hikari.HikariDataSource
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import org.slf4j.LoggerFactory
import java.sql.ResultSet
import java.sql.SQLException
import java.time.OffsetDateTime
import java.util.UUID
import java.util.Optional
import java.util.function.Function
import java.util.function.Supplier
import java.util.stream.Stream


class PostgresDataManager(private val hds: HikariDataSource) {
    companion object {
        private val logger = LoggerFactory.getLogger(PostgresDataManager::class.java)
    }

    fun getEntitiesInEntitySetQuery(entitySetId: UUID, authorizedPropertyTypes: Set<PropertyType>): String {
        val esTableName = DataTables.quote(DataTables.entityTableName(entitySetId))

        var subQueries = StringBuilder("")
        val where = StringBuilder("")

        authorizedPropertyTypes.forEach { pt ->
            val fqn = DataTables.quote(pt.type.fullQualifiedNameAsString)
            val ptTableName = DataTables.quote(DataTables.propertyTableName(pt.id))
            subQueries = subQueries
                    .append(" LEFT JOIN (SELECT id, array_agg(")
                    .append(fqn)
                    .append(") as ")
                    .append(ptTableName)
                    .append(" FROM ")
                    .append(ptTableName)
                    .append(" WHERE entity_set_id='")
                    .append(entitySetId.toString())
                    .append("' AND version > 0 group by id) as ")
                    .append(ptTableName)
                    .append(" USING (id) ")
        }

        return StringBuilder("SELECT * FROM ").append(esTableName).append(subQueries).append(where)
                .append(";").toString()
    }

    @SuppressFBWarnings(value = ["ODR_OPEN_DATABASE_RESOURCE"], justification = "Connection handled by CountdownConnectionCloser")
    fun getEntitiesInEntitySet(entitySetId: UUID, authorizedPropertyTypes: Set<PropertyType>): Stream<Entity> {
        val authorizedPropertyTypeIds = authorizedPropertyTypes.map(PropertyType::getId).toSet()

        return PostgresIterable(
                Supplier {
                    try {
                        val connection = hds.connection
                        val stmt = connection.createStatement()
                        val rs = stmt.executeQuery(getEntitiesInEntitySetQuery(entitySetId, authorizedPropertyTypes))
                        StatementHolder(connection, stmt, rs)
                    } catch (e: SQLException) {
                        logger.error("Unable to create statement holder!", e)
                        throw IllegalStateException ("Unable to create statement holder.", e)
                    }

                }, Function<ResultSet, Entity> { rs ->
                    try {
                        return@Function ResultSetAdapters.entity(rs, authorizedPropertyTypeIds)
                    } catch ( e: SQLException) {
                        logger.error("Unable to load entity information.", e)
                        throw IllegalStateException ("Unable to load entity information.", e)
                    }
        }).stream()
    }

    fun markAsIndexed(entityKeyIds: Map<UUID, Optional<Set<UUID>>>, linking: Boolean): Int {
        return updateLastIndex(entityKeyIds, linking, OffsetDateTime.now())
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

    fun markAsLinked(entitySetId: UUID, processedEntities: Set<UUID>): Int {
        hds.connection.use {
            it.prepareStatement(updateLastLinkSql(entitySetId)).use {
                val arr = PostgresArrays.createUuidArray(it.connection, processedEntities)
                it.setObject(1, OffsetDateTime.now())
                it.setArray(2, arr)
                return it.executeUpdate()
            }

        }
    }

    fun markAsProcessed(entitySetId: UUID, processedEntities: Set<UUID>, processedTime: OffsetDateTime): Int {
        hds.connection.use {
            it.prepareStatement(updateLastPropagateSql(entitySetId)).use {
                val arr = PostgresArrays.createUuidArray(it.connection, processedEntities)
                it.setObject(1, processedTime)
                it.setArray(2, arr)
                return it.executeUpdate()
            }

        }
    }

    fun updateLastIndexSql(idsByEntitySetId: Map<UUID, Optional<kotlin.collections.Set<UUID>>>): String {
        val entitiesClause = buildEntitiesClause(idsByEntitySetId, false)

        return "UPDATE ${PostgresTable.IDS.name} SET ${DataTables.LAST_INDEX.name} = ? " +
                "WHERE $entitiesClause "
    }

    fun updateLastLinkIndexSql(idsByEntitySetId: Map<UUID, Optional<kotlin.collections.Set<UUID>>>): String {
        val entitiesClause = buildEntitiesClause(idsByEntitySetId, true)

        return "UPDATE ${PostgresTable.IDS.name} SET ${PostgresColumn.LAST_LINK_INDEX.name} = ? WHERE TRUE $entitiesClause"
    }

    fun updateLastLinkSql(entitySetId: UUID): String {
        return "UPDATE ${PostgresTable.IDS.name} SET ${DataTables.LAST_LINK.name} = ? " +
                "WHERE ${PostgresColumn.ENTITY_SET_ID.name} = '$entitySetId' AND ${PostgresColumn.ID.name} IN (SELECT UNNEST( (?)::uuid[] ))"
    }

    fun updateLastPropagateSql(entitySetId: UUID): String {
        return "UPDATE ${PostgresTable.IDS.name} SET ${PostgresColumn.LAST_PROPAGATE.name} = ? " +
                "WHERE ${PostgresColumn.ENTITY_SET_ID.name} = '$entitySetId' AND ${PostgresColumn.ID.name} IN (SELECT UNNEST( (?)::uuid[] ))"
    }
}
