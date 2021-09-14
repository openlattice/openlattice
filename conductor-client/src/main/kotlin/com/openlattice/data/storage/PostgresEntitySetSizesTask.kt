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

import com.openlattice.data.storage.PostgresEntitySetSizesInitializationTask.Companion.REFRESH_ENTITY_SET_COUNTS_VIEW
import com.openlattice.tasks.HazelcastFixedRateTask
import com.openlattice.tasks.Task
import org.slf4j.LoggerFactory
import java.util.concurrent.TimeUnit

private val logger = LoggerFactory.getLogger(PostgresEntitySetSizesTask::class.java)

class PostgresEntitySetSizesTask : HazelcastFixedRateTask<PostgresEntitySetSizesTaskDependency> {

    override fun getInitialDelay(): Long {
        return 0
    }

    override fun getPeriod(): Long {
        return 60_000 * 60 // 15 minutes
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
        return Task.POSTGRES_ENTITY_SET_SIZES_REFRESH_TASK.name
    }

    override fun getDependenciesClass(): Class<out PostgresEntitySetSizesTaskDependency> {
        return PostgresEntitySetSizesTaskDependency::class.java
    }

}
