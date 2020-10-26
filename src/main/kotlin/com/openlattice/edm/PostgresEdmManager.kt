/*
 * Copyright (C) 2017. OpenLattice, Inc
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@openlattice.com
 *
 */
package com.openlattice.edm

import com.openlattice.data.PropertyUsageSummary
import com.openlattice.postgres.PostgresArrays
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresTable.DATA
import com.openlattice.postgres.PostgresTable.ENTITY_SETS
import com.openlattice.postgres.ResultSetAdapters
import com.openlattice.postgres.streams.BasePostgresIterable
import com.openlattice.postgres.streams.PreparedStatementHolderSupplier
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.LoggerFactory
import java.sql.SQLException
import java.util.*

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class PostgresEdmManager(private val hds: HikariDataSource) {
    fun getPropertyUsageSummary(propertyTypeId: UUID?): Iterable<PropertyUsageSummary> {
        val wrappedEntitySetsTableName = "wrapped_entity_sets"

        val getPropertyTypeSummary = """
          WITH $wrappedEntitySetsTableName AS (
            SELECT ${ENTITY_TYPE_ID.name}, ${NAME.name} AS entity_set_name, ${ID.name}
            FROM ${ENTITY_SETS.name}
         ) SELECT ${ENTITY_TYPE_ID.name}, entity_set_name, ${ENTITY_SET_ID.name}, COUNT(*)
           FROM ${DATA.name}
           LEFT JOIN $wrappedEntitySetsTableName
             ON ${ENTITY_SET_ID.name} = $wrappedEntitySetsTableName.${ID.name}
             WHERE ${VERSION.name} > 0 AND ${PROPERTY_TYPE_ID.name} = ?
             GROUP BY ( ${ENTITY_TYPE_ID.name}, entity_set_name, ${ENTITY_SET_ID.name} )
        """.trimIndent()

        return BasePostgresIterable(PreparedStatementHolderSupplier(hds, getPropertyTypeSummary) { ps ->
            ps.setObject(1, propertyTypeId)
        }) { rs ->
            try {
                ResultSetAdapters.propertyUsageSummary(rs)
            } catch (e: SQLException) {
                logger.error("Unable to load property summary information.", e)
                throw IllegalStateException("Unable to load property summary information.", e)
            }
        }
    }

    fun countEntitySetsOfEntityTypes(entityTypeIds: Set<UUID?>?): Map<UUID, Long> {
        val query = """
            SELECT ${ENTITY_TYPE_ID.name}, COUNT(*)
            FROM ${ENTITY_SETS.name}
            WHERE ${ENTITY_TYPE_ID.name} = ANY(?) 
            GROUP BY ${ENTITY_TYPE_ID.name}
        """.trimIndent()

        return try {
            BasePostgresIterable(PreparedStatementHolderSupplier(hds, query) { ps ->
                val arr = PostgresArrays.createUuidArray(ps.connection, entityTypeIds)
                ps.setArray(1, arr)
            }) {
                ResultSetAdapters.entityTypeId(it) to ResultSetAdapters.count(it)
            }.toMap()
        } catch (e: SQLException) {
            logger.debug("Unable to count entity sets for entity type ids {}", entityTypeIds, e)
            java.util.Map.of()
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(PostgresEdmManager::class.java)
    }
}