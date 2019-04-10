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
package com.openlattice.rehearsal.organization

import com.openlattice.assembler.AssemblerConfiguration
import com.openlattice.assembler.AssemblerConnectionManager
import com.openlattice.assembler.pods.AssemblerConfigurationPod
import com.openlattice.edm.EntitySet
import com.openlattice.edm.type.PropertyType
import com.openlattice.postgres.DataTables
import com.openlattice.postgres.PostgresColumn
import com.openlattice.postgres.PostgresTable
import com.openlattice.rehearsal.application.TestServer
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import java.sql.ResultSet
import java.util.*

class TestAssemblerConnectionManager {

    companion object {
        private var assemblerConfiguration: AssemblerConfiguration

        init {
            val testsServer = TestServer(AssemblerConfigurationPod::class.java)
            testsServer.sprout("local")
            assemblerConfiguration = testsServer.context.getBean(AssemblerConfiguration::class.java)
        }

        @JvmStatic
        fun connect(organizationId: UUID): HikariDataSource {
            val dbName = "org_${organizationId.toString().replace("-", "").toLowerCase()}"
            val config = assemblerConfiguration.server.clone() as Properties
            config.computeIfPresent("jdbcUrl") { _, jdbcUrl ->
                "${(jdbcUrl as String).removeSuffix(
                        "/"
                )}/$dbName" + if (assemblerConfiguration.ssl) {
                    "?ssl=true"
                } else {
                    ""
                }
            }
            return HikariDataSource(HikariConfig(config))
        }


        /**
         * Generates SQL query for selecting properties of materialized entity sets.
         * If properties are left empty, the query will select all columns.
         */
        @JvmStatic
        fun selectFromEntitySetSql(entitySet: EntitySet, properties: Set<PropertyType> = setOf()): String {
            val columnsToSelect = if (properties.isEmpty()) {
                "*"
            } else {
                properties.map { it.type.fullQualifiedNameAsString }.joinToString(",") { DataTables.quote(it) }
            }
            return "SELECT $columnsToSelect FROM " +
                    "${AssemblerConnectionManager.MATERIALIZED_VIEWS_SCHEMA}.${DataTables.quote(entitySet.name)}"
        }

        /**
         * Generates SQL query for selecting all edges that contain any of the entity set ids provided.
         * If entitySetIds are left empty, the query will select all rows.
         */
        @JvmStatic
        fun selectEdgesOfEntitySetsSql(entitySetIds: Set<UUID> = setOf()): String {
            return "SELECT * FROM ${AssemblerConnectionManager.MATERIALIZED_VIEWS_SCHEMA}.${PostgresTable.EDGES.name} " +
                    if (entitySetIds.isEmpty()) {
                        ""
                    } else {
                        val setIdsClause = entitySetIds.joinToString("','", "('", "')")
                        "WHERE ${PostgresColumn.SRC_ENTITY_SET_ID.name} IN $setIdsClause " +
                                "OR ${PostgresColumn.EDGE_ENTITY_SET_ID.name} IN $setIdsClause " +
                                "OR ${PostgresColumn.DST_ENTITY_SET_ID.name} IN $setIdsClause"
                    }
        }
    }
}