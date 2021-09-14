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

import com.google.common.collect.MapMaker
import com.hazelcast.config.*
import com.openlattice.assembler.EntitySetAssemblyKey
import com.openlattice.assembler.MaterializedEntitySet
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.organization.OrganizationEntitySetFlag
import com.openlattice.postgres.PostgresArrays
import com.openlattice.postgres.PostgresColumn.ORGANIZATION_ID
import com.openlattice.postgres.PostgresTable
import com.openlattice.postgres.ResultSetAdapters
import com.zaxxer.hikari.HikariDataSource
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.time.OffsetDateTime
import java.time.temporal.ChronoUnit
import java.util.*
import kotlin.random.Random

open class MaterializedEntitySetMapStore(
        hds: HikariDataSource
) : AbstractBasePostgresMapstore<EntitySetAssemblyKey, MaterializedEntitySet>(
        HazelcastMap.MATERIALIZED_ENTITY_SETS,
        PostgresTable.MATERIALIZED_ENTITY_SETS,
        hds
) {

    companion object {
        @JvmStatic
        val ORGANIZATION_ID_INDEX = "__key.organizationId"

        @JvmStatic
        val ENTITY_SET_ID_INDEX = "__key.entitySetId"

        @JvmStatic
        val FLAGS_INDEX = "flags[any]"

        @JvmStatic
        val LAST_REFRESH_INDEX = "lastRefresh"

        @JvmStatic
        val REFRESH_RATE_INDEX = "refreshRate"

        private val testKey = EntitySetAssemblyKey(UUID.randomUUID(), UUID.randomUUID())
    }

    override fun bind(ps: PreparedStatement, key: EntitySetAssemblyKey, value: MaterializedEntitySet) {
        val flags = PostgresArrays.createTextArray(ps.connection, value.flags.map { it.toString() })

        bind(ps, key, 1)
        ps.setArray(3, flags)
        ps.setObject(4, value.refreshRate)
        ps.setObject(5, value.lastRefresh)

        // UPDATE
        ps.setArray(6, flags)
        ps.setObject(7, value.refreshRate)
        ps.setObject(8, value.lastRefresh)
    }

    override fun bind(ps: PreparedStatement, key: EntitySetAssemblyKey, offset: Int): Int {
        ps.setObject(offset, key.entitySetId)
        ps.setObject(offset + 1, key.organizationId)
        return offset + 2
    }

    override fun mapToKey(rs: ResultSet): EntitySetAssemblyKey {
        return ResultSetAdapters.entitySetAssemblyKey(rs)
    }

    override fun mapToValue(rs: ResultSet): MaterializedEntitySet {
        return ResultSetAdapters.materializedEntitySet(rs)
    }

    override fun getMapConfig(): MapConfig {
        return super.getMapConfig()
                .addIndexConfig(IndexConfig(IndexType.HASH, ORGANIZATION_ID_INDEX))
                .addIndexConfig(IndexConfig(IndexType.HASH, ENTITY_SET_ID_INDEX))
                .addIndexConfig(IndexConfig(IndexType.HASH,FLAGS_INDEX))
                .addIndexConfig(IndexConfig(IndexType.HASH,LAST_REFRESH_INDEX))
                .addIndexConfig(IndexConfig(IndexType.HASH,REFRESH_RATE_INDEX))
                .setInMemoryFormat(InMemoryFormat.OBJECT)
    }

    fun loadMaterializedEntitySetsForOrganization(
            connection: Connection,
            organizationId: UUID
    ): Map<UUID, EnumSet<OrganizationEntitySetFlag>> {
        val result = MapMaker().makeMap<UUID, EnumSet<OrganizationEntitySetFlag>>()

        val selectInQuery = table.selectInQuery(listOf(), listOf(ORGANIZATION_ID), 1)
        connection.prepareStatement(selectInQuery).use { selectIn ->
            selectIn.setObject(1, organizationId)
            val results = selectIn.executeQuery()
            while (results.next()) {
                val materializedEntitySet = mapToValue(results)
                result[materializedEntitySet.assemblyKey.entitySetId] = materializedEntitySet.flags
            }
        }

        return result
    }

    override fun generateTestKey(): EntitySetAssemblyKey {
        return testKey
    }

    override fun generateTestValue(): MaterializedEntitySet {
        val organizationEntitySetFlags = OrganizationEntitySetFlag.values()
        val refreshRate = Random.nextLong()
        val flags = EnumSet.noneOf(OrganizationEntitySetFlag::class.java)
        if (Random.nextBoolean()) {
            (0 until Random.nextInt(2, organizationEntitySetFlags.size))
                    .forEach { flags.add(organizationEntitySetFlags[it]) }
        }
        val lastRefresh = OffsetDateTime.now().plus(Random.nextLong(90000000000L), ChronoUnit.MILLIS)

        return MaterializedEntitySet(testKey, refreshRate, flags, lastRefresh)
    }
}