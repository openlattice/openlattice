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
import com.openlattice.postgres.PostgresColumn
import com.openlattice.postgres.PostgresTable
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
        val getPropertyTypeSummary = String.format("WITH %1\$s AS (SELECT %2\$s, %3\$s AS %4\$s, %5\$s FROM %6\$s) " +
                "SELECT %2\$s, %4\$s, %7\$s, COUNT(*) FROM %8\$s LEFT JOIN %1\$s ON %7\$s = %1\$s.id " +
                "WHERE %9\$s > 0 AND %10\$s = ? GROUP BY ( %2\$s , %4\$s, %7\$s )",
                wrappedEntitySetsTableName,
                PostgresColumn.ENTITY_TYPE_ID.name,
                PostgresColumn.NAME.name,
                PostgresColumn.ENTITY_SET_NAME.name,
                PostgresColumn.ID.name,
                PostgresTable.ENTITY_SETS.name,
                PostgresColumn.ENTITY_SET_ID.name,
                PostgresTable.DATA.name,
                PostgresColumn.VERSION.name,
                PostgresColumn.PROPERTY_TYPE_ID.name
        )
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
        val query = "SELECT " + PostgresColumn.ENTITY_TYPE_ID.name + ", COUNT(*) FROM " + PostgresTable.ENTITY_SETS.name + " WHERE " + PostgresColumn.ENTITY_TYPE_ID
                .name + " = ANY( ? ) GROUP BY " + PostgresColumn.ENTITY_TYPE_ID.name

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