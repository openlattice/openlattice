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
package com.openlattice.postgres.mapstores

import com.hazelcast.config.InMemoryFormat
import com.hazelcast.config.MapConfig
import com.hazelcast.config.MapIndexConfig
import com.openlattice.assembler.MaterializedEntitySet
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.organization.OrganizationEntitySetFlag
import com.openlattice.postgres.PostgresArrays
import com.openlattice.postgres.PostgresTable
import com.openlattice.postgres.ResultSetAdapters
import com.zaxxer.hikari.HikariDataSource
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.util.UUID
import kotlin.random.Random

open class MaterializedEntitySetMapStore(
        hds: HikariDataSource
) : AbstractBasePostgresMapstore<UUID, MaterializedEntitySet>(
        HazelcastMap.MATERIALIZED_ENTITY_SETS.name,
        PostgresTable.MATERIALIZED_ENTITY_SETS,
        hds) {

    companion object {
        @JvmStatic
        val ORGANIZATION_INDEX = "organizationId"
        private val testKey = UUID.randomUUID()
    }

    override fun bind(ps: PreparedStatement, key: UUID, value: MaterializedEntitySet) {
        val flags = PostgresArrays.createTextArray(ps.connection, value.flags.map { it.toString() })

        bind(ps, key, 1)
        ps.setObject(2, value.organizationId)
        ps.setArray(3, flags)

        // UPDATE
        ps.setObject(4, value.organizationId)
        ps.setArray(5, flags)
    }

    override fun bind(ps: PreparedStatement, key: UUID, offset: Int): Int {
        ps.setObject(offset, key)
        return offset + 1
    }

    override fun mapToKey(rs: ResultSet): UUID {
        return ResultSetAdapters.organizationId(rs)
    }

    override fun mapToValue(rs: ResultSet): MaterializedEntitySet {
        return ResultSetAdapters.materializedEntitySet(rs)
    }

    override fun getMapConfig(): MapConfig {
        return super.getMapConfig()
                .addMapIndexConfig(MapIndexConfig(ORGANIZATION_INDEX, false))
                .setInMemoryFormat(InMemoryFormat.OBJECT)
    }

    override fun generateTestKey(): UUID {
        return testKey
    }

    override fun generateTestValue(): MaterializedEntitySet {
        val organizationEntitySetFlags = OrganizationEntitySetFlag.values()
        val flags = if (Random.nextBoolean()) {
            (0..Random.nextInt(1, organizationEntitySetFlags.size))
                    .map { organizationEntitySetFlags[it] }
                    .toMutableSet()
        } else {
            mutableSetOf()
        }

        return MaterializedEntitySet(testKey, UUID.randomUUID(), flags)
    }
}