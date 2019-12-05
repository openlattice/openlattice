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

package com.openlattice.organizations.tasks

import com.google.common.base.Stopwatch
import com.openlattice.IdConstants.GLOBAL_ORGANIZATION_ID
import com.openlattice.assembler.tasks.ProductionViewSchemaInitializationTask
import com.openlattice.assembler.tasks.UsersAndRolesInitializationTask
import com.openlattice.authorization.SystemRole
import com.openlattice.authorization.initializers.AuthorizationInitializationTask
import com.openlattice.authorization.initializers.AuthorizationInitializationTask.Companion.GLOBAL_ADMIN_ROLE
import com.openlattice.authorization.initializers.AuthorizationInitializationTask.Companion.GLOBAL_USER_ROLE
import com.openlattice.organization.OrganizationConstants.Companion.GLOBAL_ORG_PRINCIPAL
import com.openlattice.organizations.Grant
import com.openlattice.organizations.GrantType
import com.openlattice.organizations.Organization
import com.openlattice.tasks.HazelcastInitializationTask
import com.openlattice.tasks.PostConstructInitializerTaskDependencies.PostConstructInitializerTask
import com.openlattice.tasks.Task
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.TimeUnit

private val logger = LoggerFactory.getLogger(OrganizationsInitializationTask::class.java)

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class OrganizationsInitializationTask : HazelcastInitializationTask<OrganizationsInitializationDependencies> {
    override fun initialize(dependencies: OrganizationsInitializationDependencies) {
        logger.info("Running bootstrap process for organizations.")
        val sw = Stopwatch.createStarted()
        val organizationService = dependencies.organizationService
        val globalOrg = organizationService.maybeGetOrganization(GLOBAL_ORG_PRINCIPAL)
        val defaultPartitions = organizationService.allocateDefaultPartitions(organizationService.numberOfPartitions)

        if (globalOrg.isPresent) {
            val orgPrincipal = globalOrg.get()
            val org = organizationService.getOrganization(orgPrincipal.id)!!
            mergeGrants(org)
            dependencies.configuration.connection.ifPresent { org.connections.addAll(it) }
            org.grants.forEach {
                (roleId, grantMap) -> grantMap.values.forEach {
                    grant -> organizationService.updateRoleGrant(orgPrincipal.id, roleId, grant)
                }
            }
            logger.info(
                    "Expected id = {}, Actual id = {}",
                    GLOBAL_ORGANIZATION_ID.id,
                    orgPrincipal.id
            )
            require(GLOBAL_ORGANIZATION_ID.id == orgPrincipal.id) {
                "Mistmatch in expected global org id and read global org id"
            }
        } else {
            val org = createGlobalOrg(defaultPartitions)
            mergeGrants(org)
            dependencies.configuration.connection.ifPresent { org.connections.addAll(it) }
            organizationService.createOrganization(
                    GLOBAL_ADMIN_ROLE.principal,
                    createGlobalOrg(defaultPartitions)
            )
        }

        logger.info("Bootstrapping for organizations took {} ms", sw.elapsed(TimeUnit.MILLISECONDS))
    }

    private fun mergeGrants(org: Organization) {
        globalGrants().forEach { (roleId, grantMap) ->
            grantMap.forEach { (grantType, grant) ->
                org.grants.getOrPut(roleId) { grantMap }[grantType] = grant
            }
        }
    }

    private fun globalGrants(): MutableMap<UUID, MutableMap<GrantType, Grant>> {
        val userPrincipal = getDependency().spm.lookupRole(GLOBAL_USER_ROLE.principal)
        val adminPrincipal = getDependency().spm.lookupRole(GLOBAL_ADMIN_ROLE.principal)
        return mutableMapOf(
                userPrincipal.id to mutableMapOf(GrantType.Automatic to Grant(GrantType.Automatic, setOf())),
                adminPrincipal.id to mutableMapOf(
                        GrantType.Roles to Grant(GrantType.Roles, setOf(SystemRole.ADMIN.principal.id))
                )
        )
    }

    override fun getInitialDelay(): Long {
        return 0
    }

    override fun getTimeUnit(): TimeUnit {
        return TimeUnit.MILLISECONDS
    }

    override fun after(): Set<Class<out HazelcastInitializationTask<*>>> {
        return setOf(
                AuthorizationInitializationTask::class.java,
                UsersAndRolesInitializationTask::class.java,
                PostConstructInitializerTask::class.java,
                ProductionViewSchemaInitializationTask::class.java
        )
    }

    override fun getName(): String {
        return Task.ORGANIZATION_INITIALIZATION.name
    }

    override fun getDependenciesClass(): Class<out OrganizationsInitializationDependencies> {
        return OrganizationsInitializationDependencies::class.java
    }

    companion object {
        private fun createGlobalOrg(partitions: List<Int>): Organization {
            val id = GLOBAL_ORGANIZATION_ID.id
            val title = "Global Organization"
            return Organization(
                    Optional.of(id),
                    GLOBAL_ORG_PRINCIPAL,
                    title,
                    Optional.empty(),
                    mutableSetOf(),
                    mutableSetOf(),
                    mutableSetOf(),
                    mutableSetOf(),
                    Optional.of(mutableSetOf()),
                    Optional.of(partitions.toMutableList())
            )
        }


    }
}
