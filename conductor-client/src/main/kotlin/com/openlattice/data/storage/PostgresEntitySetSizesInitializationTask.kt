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
import com.openlattice.postgres.PostgresColumn.COUNT
import com.openlattice.postgres.PostgresColumn.ENTITY_SET_ID
import com.openlattice.postgres.PostgresColumn.FLAGS
import com.openlattice.postgres.PostgresColumn.ID
import com.openlattice.postgres.PostgresColumn.LINKED_ENTITY_SETS
import com.openlattice.postgres.PostgresColumn.LINKING_ID
import com.openlattice.postgres.PostgresColumn.VERSION
import com.openlattice.postgres.PostgresTable.ENTITY_SETS
import com.openlattice.postgres.PostgresTable.IDS
import com.openlattice.tasks.HazelcastInitializationTask
import com.openlattice.tasks.Task
import org.slf4j.LoggerFactory

// todo change name back to "entity_set_counts"

private val logger = LoggerFactory.getLogger(PostgresEntitySetSizesInitializationTask::class.java)

class PostgresEntitySetSizesInitializationTask : HazelcastInitializationTask<PostgresEntitySetSizesTaskDependency> {

    companion object {

        const val ENTITY_SET_SIZES_VIEW = "entity_set_counts"

        val CREATE_ENTITY_SET_COUNTS_VIEW = "CREATE MATERIALIZED VIEW IF NOT EXISTS $ENTITY_SET_SIZES_VIEW " +
                "AS $NORMAL_ENTITY_SET_COUNTS " // TODO bring back linking counts

        const val REFRESH_ENTITY_SET_COUNTS_VIEW = "REFRESH MATERIALIZED VIEW $ENTITY_SET_SIZES_VIEW "
    }

    override fun getInitialDelay(): Long {
        return 0
    }

    override fun initialize(dependencies: PostgresEntitySetSizesTaskDependency) {
        logger.info("Creating entity set count views.")
        val connection = getDependency().hikariDataSource.connection
        connection.use {
            it.createStatement().use { stmt ->
                stmt.execute(CREATE_ENTITY_SET_COUNTS_VIEW)
            }
        }
    }

    override fun after(): Set<Class<out HazelcastInitializationTask<*>>> {
        return setOf()
    }

    override fun getName(): String {
        return Task.POSTGRES_ENTITY_SET_SIZES_INITIALIZATION.name
    }

    override fun getDependenciesClass(): Class<out PostgresEntitySetSizesTaskDependency> {
        return PostgresEntitySetSizesTaskDependency::class.java
    }
}

private val NORMAL_ENTITY_SET_COUNTS =
        "( SELECT ${ENTITY_SET_ID.name}, COUNT(*) as $COUNT FROM ${IDS.name} WHERE ${VERSION.name} > 0 GROUP BY (${ENTITY_SET_ID.name}) )"

private const val WRAPPED_LINKED_ENTITY_SETS = "wrapped_linked_entity_sets"
private val WITH_WRAPPED_LINKED_ENTITY_SETS = "WITH $WRAPPED_LINKED_ENTITY_SETS AS " +
        "(SELECT ${ID.name}, ${LINKED_ENTITY_SETS.name} FROM ${ENTITY_SETS.name} WHERE '${EntitySetFlag.LINKING}' = ANY(${FLAGS.name}) ) "

private val LINKED_ENTITY_SET_COUNTS =
        "($WITH_WRAPPED_LINKED_ENTITY_SETS " +
                "SELECT ${ID.name} as ${ENTITY_SET_ID.name}, COUNT(DISTINCT ${LINKING_ID.name}) as $COUNT FROM " +
                "( SELECT DISTINCT ${ENTITY_SET_ID.name}, ${LINKING_ID.name} FROM ${IDS.name} WHERE ${LINKING_ID.name} IS NOT NULL AND ${VERSION.name} > 0 ) as linking_ids " +
                "INNER JOIN $WRAPPED_LINKED_ENTITY_SETS " +
                "ON ( linking_ids.${ENTITY_SET_ID.name} = ANY($WRAPPED_LINKED_ENTITY_SETS.${LINKED_ENTITY_SETS.name}) ) " +
                "GROUP BY $WRAPPED_LINKED_ENTITY_SETS.${ID.name} )"