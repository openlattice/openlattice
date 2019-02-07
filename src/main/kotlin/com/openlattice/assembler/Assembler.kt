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

package com.openlattice.assembler

import com.hazelcast.core.HazelcastInstance
import com.openlattice.assembler.AssemblerConnectionManager.Companion.connect
import com.openlattice.assembler.processors.InitializeOrganizationAssemblyProcessor
import com.openlattice.authorization.AuthorizationManager
import com.openlattice.authorization.DbCredentialService
import com.openlattice.edm.EntitySet
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.organization.Organization
import com.openlattice.organizations.roles.SecurePrincipalsManager
import com.openlattice.postgres.DataTables.quote
import com.openlattice.postgres.PostgresColumn
import com.openlattice.postgres.PostgresColumn.PRINCIPAL_TYPE
import com.openlattice.postgres.PostgresTable
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.LoggerFactory
import java.util.*

const val SCHEMA = "openlattice"
const val PRODUCTION = "olprod"
private val logger = LoggerFactory.getLogger(Assembler::class.java)

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class Assembler(
        private val assemblerConfiguration: AssemblerConfiguration,
        val authz: AuthorizationManager,
        private val dbCredentialService: DbCredentialService,
        val hds: HikariDataSource,
        hazelcast: HazelcastInstance
) {
    private val entitySets = hazelcast.getMap<UUID, EntitySet>(HazelcastMap.ENTITY_SETS.name)
    private val assemblies = hazelcast.getMap<UUID, OrganizationAssembly>(HazelcastMap.ENTITY_SETS.name)

    private val target = connect("postgres")

    fun initializeRolesAndUsers(spm: SecurePrincipalsManager) {

    }





    fun materializeEntityTypes(datasource: HikariDataSource) {
        TODO("Materialize entity types")
    }

    fun materializePropertyTypes(datasource: HikariDataSource) {
        TODO("Materialize property types")
    }






    fun getMaterializedEntitySets( organizationId: UUID) : Set<UUID> {
        return assemblies[organizationId]?.entitySetIds ?: setOf()
    }
    fun getOrganizationAssembly(organizationId: UUID): OrganizationAssembly {
        return assemblies[organizationId]!!
    }

    private fun dropDatabase(organizationId: UUID, dbname: String) {
        val db = quote(dbname)
        val dbRole = quote("${dbname}_role")
        val unquotedDbAdminUser = buildUserId(organizationId)
        val dbAdminUser = quote(unquotedDbAdminUser)
        val dbAdminUserPassword = dbCredentialService.createUser(unquotedDbAdminUser)

        val dropDbUser = "DROP ROLE $dbAdminUser"
        //TODO: If we grant this role to other users, we need to make sure we drop it
        val dropDbRole = "DROP ROLE $dbRole"
        val dropDb = " DROP DATABASE $db"


        //We connect to default db in order to do initial db setup

        target.connection.use { connection ->
            connection.createStatement().use { statement ->
                statement.execute(dropDbUser)
                statement.execute(dropDbRole)
                statement.execute(dropDb)
                return@use
            }
        }
    }

    fun createOrganization(organization: Organization) {
        assemblies.set( organization.id , OrganizationAssembly(organization.id, organization.principal.id) )
        assemblies.executeOnKey(organization.id, InitializeOrganizationAssemblyProcessor())
    }
}


private val PRINCIPALS_SQL = "SELECT acl_key FROM principals WHERE ${PRINCIPAL_TYPE.name} = ?"

private val INSERT_MATERIALIZED_ENTITY_SET = "INSERT INTO ${PostgresTable.ORGANIZATION_ASSEMBLIES.name} (?,?) ON CONFLICT DO NOTHING"
private val SELECT_MATERIALIZED_ENTITY_SETS = "SELECT * FROM ${PostgresTable.ORGANIZATION_ASSEMBLIES.name} " +
        "WHERE ${PostgresColumn.ORGANIZATION_ID.name} = ?"