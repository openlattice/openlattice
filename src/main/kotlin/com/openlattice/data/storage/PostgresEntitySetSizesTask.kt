/*
 * Copyright (C) 2019. OpenLattice, Inc.
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
 *
 */
package com.openlattice.data.storage

import com.openlattice.edm.set.EntitySetFlag
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresTable.IDS
import com.openlattice.postgres.PostgresTable.ENTITY_SETS
import com.openlattice.tasks.HazelcastFixedRateTask
import com.openlattice.tasks.HazelcastInitializationTask
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.TimeUnit


private const val ENTITY_SET_SIZES_TABLE = "entity_set_counts"
private val logger = LoggerFactory.getLogger(PostgresEntitySetSizesTask::class.java)

class PostgresEntitySetSizesTask : HazelcastFixedRateTask<PostgresEntitySetSizesTaskDependency> {
    /*init {
        logger.info("Creating entity set count views.")
        val connection = getDependency().hikariDataSource.connection
        connection.use {
            it.createStatement().use { stmt ->
                stmt.execute(CREATE_ENTITY_SET_COUNTS_VIEW)
            }
        }
    }*/

    override fun getInitialDelay(): Long {
        return 0
    }

    override fun getPeriod(): Long {
        return 60000
    }

    override fun getTimeUnit(): TimeUnit {
        return TimeUnit.MILLISECONDS
    }

    override fun runTask() {
        logger.info("Refreshing entity set count views.")
        getDependency().hikariDataSource.connection.use { connection ->
            connection.createStatement().use { stmt ->
                stmt.execute(REFRESH_ENTITY_SET_COUNTS_VIEW)
            }
        }
    }

    override fun getName(): String {
        return "entity_set_sizes_refresh_task"
    }

    override fun getDependenciesClass(): Class<out PostgresEntitySetSizesTaskDependency> {
        return PostgresEntitySetSizesTaskDependency::class.java
    }

    fun getEntitySetSize(entitySetId: UUID): Long {
        return getDependency().hikariDataSource.connection.use { connection ->
            connection.prepareStatement(GET_ENTITY_SET_COUNT).use { ps ->
                ps.setObject(1, entitySetId)
                ps.executeQuery().use { rs ->
                    if (rs.next()) {
                        rs.getLong(1)
                    } else {
                        0
                    }
                }
            }
        }
    }
}

private val NORMAL_ENTITY_SET_COUNTS =
        "( SELECT ${ENTITY_SET_ID.name}, COUNT(*) as $COUNT FROM ${IDS.name} " +
                "GROUP BY (${ENTITY_SET_ID.name}) )"

private val LINKED_ENTITY_SET_COUNTS =
        "( SELECT  ${ID.name} as ${ENTITY_SET_ID.name}, COUNT(DISTINCT ${LINKING_ID.name}) as $COUNT FROM " +
                "( SELECT DISTINCT ${ENTITY_SET_ID.name}, ${LINKING_ID.name} FROM ${IDS.name} WHERE ${LINKING_ID.name} IS NOT NULL ) as linking_ids " +
                "INNER JOIN " +
                "( SELECT ${ID.name}, ${LINKED_ENTITY_SETS.name} FROM ${ENTITY_SETS.name} WHERE '${EntitySetFlag.LINKING}' = ANY(${FLAGS.name}) ) as linking_sets " +
                "ON ( linking_ids.${ENTITY_SET_ID.name} = ANY(linking_sets.${LINKED_ENTITY_SETS.name}) ) " +
                "GROUP BY linking_sets.${ID.name} )"

private val CREATE_ENTITY_SET_COUNTS_VIEW = "CREATE MATERIALIZED VIEW IF NOT EXISTS $ENTITY_SET_SIZES_TABLE " +
        "AS $NORMAL_ENTITY_SET_COUNTS UNION $LINKED_ENTITY_SET_COUNTS"

private const val REFRESH_ENTITY_SET_COUNTS_VIEW = "REFRESH MATERIALIZED VIEW $ENTITY_SET_SIZES_TABLE"

private val GET_ENTITY_SET_COUNT = "SELECT $COUNT FROM $ENTITY_SET_SIZES_TABLE WHERE ${ENTITY_SET_ID.name} = ?"
