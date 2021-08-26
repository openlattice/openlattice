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

import com.kryptnostic.rhizome.pods.ConfigurationLoaderPod
import com.openlattice.assembler.AssemblerConfiguration
import com.openlattice.assembler.AssemblerConnectionManager
import com.openlattice.assembler.PostgresDatabases
import com.openlattice.assembler.pods.AssemblerConfigurationPod
import com.openlattice.postgres.DataTables
import com.openlattice.postgres.PostgresColumn
import com.openlattice.postgres.PostgresTable
import com.openlattice.rehearsal.application.TestServer
import com.zaxxer.hikari.HikariDataSource
import java.sql.ResultSet
import java.util.*

class TestAssemblerConnectionManager {

    companion object {
        private var assemblerConfiguration: AssemblerConfiguration

        // TODO change tests for this after transporter is ready
        const val PRODUCTION_FOREIGN_SCHEMA = "prod"

        init {
            val testsServer = TestServer(ConfigurationLoaderPod::class.java, AssemblerConfigurationPod::class.java)
            testsServer.sprout("local")
            assemblerConfiguration = testsServer.context.getBean(AssemblerConfiguration::class.java)
        }

        @JvmStatic
        fun connect(organizationId: UUID, config: Optional<Properties> = Optional.empty()): HikariDataSource {
            val dbName = PostgresDatabases.buildDefaultOrganizationDatabaseName(organizationId)
            val connectionConfig = config.orElse(assemblerConfiguration.server.clone() as Properties)

            return AssemblerConnectionManager.createDataSource(dbName, connectionConfig, assemblerConfiguration.ssl)
        }

        /**
         * Generates SQL query for selecting properties of materialized entity sets.
         * If properties are left empty, the query will select all columns.
         */
        @JvmStatic
        fun selectFromEntitySetSql(entitySetName: String, properties: Set<String> = setOf()): String {
            val columnsToSelect = if (properties.isEmpty()) {
                "*"
            } else {
                properties.joinToString(",") { DataTables.quote(it) }
            }
            return "SELECT $columnsToSelect FROM ${AssemblerConnectionManager.entitySetNameTableName(entitySetName)}"
        }

        /**
         * Generates SQL query for selecting all edges that contain any of the entity set ids provided.
         * If entitySetIds are left empty, the query will select all rows.
         */
        @JvmStatic
        fun selectEdgesOfEntitySetsSql(
                srcEntitySetId: Optional<UUID> = Optional.empty(),
                edgeEntitySetId: Optional<UUID> = Optional.empty(),
                dstEntitySetId: Optional<UUID> = Optional.empty()
        ): String {
            return "SELECT * FROM ${AssemblerConnectionManager.OPENLATTICE_SCHEMA}.${PostgresTable.E.name} " +
                    if (!srcEntitySetId.isPresent && !edgeEntitySetId.isPresent && !dstEntitySetId.isPresent) {
                        ""
                    } else {
                        val srcEntitySetIdClause = if (srcEntitySetId.isPresent) {
                            "${PostgresColumn.SRC_ENTITY_SET_ID.name} = '${srcEntitySetId.get()}' AND "
                        } else {
                            "TRUE AND "
                        }
                        val edgeEntitySetIdClause = if (edgeEntitySetId.isPresent) {
                            "${PostgresColumn.EDGE_ENTITY_SET_ID.name} = '${edgeEntitySetId.get()}' AND "
                        } else {
                            "TRUE AND "
                        }
                        val dstEntitySetIdClause = if (dstEntitySetId.isPresent) {
                            "${PostgresColumn.DST_ENTITY_SET_ID.name} = '${dstEntitySetId.get()}'"
                        } else {
                            "TRUE"
                        }
                        "WHERE $srcEntitySetIdClause $edgeEntitySetIdClause $dstEntitySetIdClause"
                    }
        }

        @JvmStatic
        fun getColumnNames(rs: ResultSet): List<String> {
            return (1..rs.metaData.columnCount).map { column -> rs.metaData.getColumnName(column) }
        }
    }
}