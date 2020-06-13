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
package com.openlattice.scheduling.mapstores

import com.dataloom.mappers.ObjectMappers
import com.hazelcast.config.InMemoryFormat
import com.hazelcast.config.MapConfig
import com.hazelcast.config.MapIndexConfig
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.hazelcast.InternalTestDataFactory
import com.openlattice.postgres.PostgresTable
import com.openlattice.postgres.ResultSetAdapters
import com.openlattice.postgres.mapstores.AbstractBasePostgresMapstore
import com.openlattice.scheduling.ScheduledTask
import com.zaxxer.hikari.HikariDataSource
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.util.*

open class ScheduledTasksMapstore(
        hds: HikariDataSource
) : AbstractBasePostgresMapstore<UUID, ScheduledTask>(
        HazelcastMap.SCHEDULED_TASKS,
        PostgresTable.SCHEDULED_TASKS,
        hds
) {

    private val testKey = UUID.randomUUID()

    companion object {
        @JvmStatic
        val SCHEDULED_DATE_TIME_INDEX = "scheduledDateTime"

        val mapper = ObjectMappers.getJsonMapper()

    }

    override fun generateTestKey(): UUID {
        return testKey
    }

    override fun generateTestValue(): ScheduledTask {
        return InternalTestDataFactory.scheduledTask(testKey)
    }

    override fun bind(ps: PreparedStatement, key: UUID, value: ScheduledTask) {
        var index = bind(ps, key, 1)

        val taskAsJson = mapper.writeValueAsString(value.task)

        ps.setObject(index++, value.scheduledDateTime)
        ps.setString(index++, value.task::class.java.name)
        ps.setString(index++, taskAsJson)

        ps.setObject(index++, value.scheduledDateTime)
        ps.setString(index++, value.task::class.java.name)
        ps.setString(index++, taskAsJson)
    }

    override fun bind(ps: PreparedStatement, key: UUID, offset: Int): Int {
        ps.setObject(offset, key)
        return offset + 1
    }

    override fun getMapConfig(): MapConfig {
        return super.getMapConfig()
                .addMapIndexConfig(MapIndexConfig(SCHEDULED_DATE_TIME_INDEX, true))
                .setInMemoryFormat(InMemoryFormat.OBJECT)
    }

    override fun mapToKey(rs: ResultSet): UUID {
        return ResultSetAdapters.id(rs)
    }

    override fun mapToValue(rs: ResultSet): ScheduledTask {
        return ResultSetAdapters.scheduledTask(rs)
    }


}