/*
 * Copyright (C) 2018. OpenLattice, Inc.
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

package com.openlattice.authorization.mapstores

import com.google.common.collect.ImmutableList
import com.hazelcast.config.MapConfig
import com.hazelcast.config.MapStoreConfig
import com.kryptnostic.rhizome.mapstores.TestableSelfRegisteringMapStore
import com.openlattice.authorization.AclKey
import com.openlattice.authorization.AclKeySet
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.mapstores.TestDataFactory
import com.openlattice.postgres.PostgresArrays
import com.openlattice.postgres.PostgresColumn.ACL_KEY
import com.openlattice.postgres.PostgresColumn.ACL_KEY_SET
import com.openlattice.postgres.PostgresDatatype
import com.openlattice.postgres.ResultSetAdapters
import com.openlattice.postgres.streams.PostgresIterable
import com.openlattice.postgres.streams.StatementHolder
import com.zaxxer.hikari.HikariDataSource
import org.springframework.stereotype.Service
import java.sql.ResultSet
import java.util.*
import java.util.function.Function
import java.util.function.Supplier

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
private val insertSql = "INSERT INTO principal_tree (${ACL_KEY.name},${ACL_KEY_SET.name}) " +
        "VALUES (?, ?) " +
        "ON CONFLICT DO NOTHING"
private val selectSql = "SELECT * FROM principal_tree WHERE ${ACL_KEY.name} IN (SELECT UNNEST( ?::uuid[] ))"
private val deleteSql = "DELETE FROM principal_tree WHERE ${ACL_KEY.name} IN (SELECT UNNEST( ?::uuid[] ))"
private val deleteNotIn = "DELETE FROM principal_tree WHERE ${ACL_KEY.name} = ? AND ${ACL_KEY_SET.name} NOT IN (SELECT UNNEST( ?::uuid[] ))"

@Service //This is here to allow this class to be automatically open for @Timed to work correctly
class PrincipalTreesMapstore(val hds: HikariDataSource) : TestableSelfRegisteringMapStore<AclKey, AclKeySet> {
    override fun getMapName(): String {
        return HazelcastMap.PRINCIPAL_TREES.name;
    }

    override fun getTable(): String {
        return "principal_trees";
    }

    override fun generateTestKey(): AclKey {
        return TestDataFactory.aclKey()
    }

    override fun generateTestValue(): AclKeySet {
        return AclKeySet(ImmutableList.of(generateTestKey(), generateTestKey(), generateTestKey()))
    }

    override fun getMapStoreConfig(): MapStoreConfig {
        return MapStoreConfig()
                .setImplementation(this)
                .setEnabled(true)
                .setWriteDelaySeconds(0)
    }

    override fun getMapConfig(): MapConfig {
        return MapConfig(mapName)
                .setMapStoreConfig(mapStoreConfig)
    }

    override fun storeAll(map: Map<AclKey, AclKeySet>) {
        hds.connection.use {
            val connection = it
            val ps = connection.prepareStatement(insertSql)
            val ps2 = connection.prepareStatement(deleteNotIn)
            map.forEach {
                val aclKey = it.key
                val arrKey = connection.createArrayOf(PostgresDatatype.UUID.sql(), (aclKey as List<UUID>).toTypedArray())
                it.value.forEach {
                    val arr = connection.createArrayOf(PostgresDatatype.UUID.sql(), (it as List<UUID>).toTypedArray())
                    ps.setObject(1, arrKey)
                    ps.setArray(2, arr)
                    ps.addBatch()

                    ps2.setObject(1, arrKey)
                    ps2.setArray(2, arr)
                    ps2.addBatch()
                }
            }
            ps.executeBatch()
            ps2.executeBatch()

        }

    }

    override fun loadAllKeys(): MutableIterable<AclKey> {
        return PostgresIterable<AclKey>(Supplier {
            val connection = hds.connection
            val stmt = connection.createStatement()
            val rs = stmt.executeQuery("SELECT * from principal_tree")
            StatementHolder(connection, stmt, rs)
        }, Function<ResultSet, AclKey> { ResultSetAdapters.aclKey(it) }
        )
    }

    override fun store(key: AclKey, value: AclKeySet) {
        storeAll(mapOf(key to value))
    }

    override fun loadAll(keys: Collection<AclKey>): MutableMap<AclKey, AclKeySet> {
        val data = PostgresIterable<Pair<AclKey, AclKey>>(
                Supplier {
                    val connection = hds.connection
                    val ps = connection.prepareStatement(selectSql)
                    val arr = PostgresArrays.createUuidArrayOfArrays(
                            connection, keys.map { (it as List<UUID>).toTypedArray() }.stream()
                    )
                    ps.setArray(1, arr)
                    StatementHolder(connection, ps, ps.executeQuery())
                },

                Function<ResultSet, Pair<AclKey, AclKey>> {
                    ResultSetAdapters.aclKey(it) to ResultSetAdapters.aclKeySetSingle(
                            it
                    )
                }
        )
        val map: MutableMap<AclKey, AclKeySet> = mutableMapOf()
        data.forEach { map.getOrPut(it.first) { AclKeySet() }.add(it.second) }
        return map
    }

    override fun deleteAll(keys: Collection<AclKey>) {
        hds.connection.use {
            val connection = it
            it.prepareStatement(deleteSql).use {
                val ps = it
                val arr = PostgresArrays.createUuidArrayOfArrays(
                        connection, keys.map { (it as List<UUID>).toTypedArray() }.stream()
                )
                it.setArray(1, arr)
                it.executeUpdate()
            }
        }
    }

    override fun load(key: AclKey): AclKeySet? {
        val loaded = loadAll(listOf(key))
        return loaded[key]
    }

    override fun delete(key: AclKey) {
        deleteAll(listOf(key))
    }

    companion object {
        const val INDEX = "index[any]"
    }
}