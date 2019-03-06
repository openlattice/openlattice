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

package com.openlattice.assembler.tasks

import com.hazelcast.query.Predicates
import com.openlattice.assembler.*
import com.openlattice.authorization.initializers.AuthorizationInitializationTask
import com.openlattice.authorization.securable.SecurableObjectType
import com.openlattice.organizations.tasks.OrganizationsInitializationTask
import com.openlattice.tasks.HazelcastInitializationTask
import com.openlattice.tasks.Task
import org.slf4j.LoggerFactory
import java.util.concurrent.TimeUnit

private val logger = LoggerFactory.getLogger(CleanOutOldUsersInitializationTask::class.java)

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class CleanOutOldUsersInitializationTask : HazelcastInitializationTask<AssemblerDependencies> {
    override fun initialize(dependencies: AssemblerDependencies) {
        logger.info("Cleaning out unnecessary users from production.")
        val users = dependencies
                .assemblerConnectionManager
                .getAllUsers(dependencies.securePrincipalsManager)
/*        val organizations =
                dependencies
                        .securableObjectTypes.keySet(Predicates.equal("this", SecurableObjectType.Organization))
                        .map { it.first() }
                        .toSet()

        dependencies.organizations.getOrganizations(organizations.stream())
                .forEach { organization ->
                    try {
                        dependencies.assemblerConnectionManager.dropOrganizationDatabase(organization.id)
                    } catch (ex: Exception) {
                        logger.error("Unable to clean out old database for organization: {}. \n\t Due to {}",
                                organization, ex.message)
                    }
//                    dependencies
//                            .assemblerConnectionManager
//                            .revokeConnectAndSchemaUsage(
//                                    dependencies.assemblerConnectionManager.connect(
//                                            organization.principal.id
//                                    ),
//                                    organization.principal.id,
//                                    user
//                            )

                }

*/
        users
                .filter { it.name != "openlattice" && it.name != "postgres" } //Just for safety
                .stream()
                .parallel().forEach { user ->
                    logger.info("Removing old user ${user.name} from production database")
                    dependencies.hds.connection.use { connection ->
                        connection.createStatement().use { stmt ->
                            stmt.execute(dropUserIfExistsSql(user.name))
                        }
                    }
                    dependencies.dbCredentialService.deleteUserCredential(user.name)
                    //Also remove from materialize views server.
                    try {

                        dependencies.assemblerConnectionManager.dropUserIfExists(user)
                    } catch (ex: Exception) {
                        logger.error("Unable to drop user: {}", user)
                    }
                    logger.info("Removed old user ${user.name} from production database")
                }
        logger.info("Cleaning out style users from materialzied view server.")
        users.map(dependencies.assemblerConnectionManager::dropUserIfExists)

    }

    override fun after(): Set<Class<out HazelcastInitializationTask<*>>> {
        return setOf(OrganizationsInitializationTask::class.java, AuthorizationInitializationTask::class.java)
    }

    override fun getInitialDelay(): Long {
        return 0
    }

    override fun getTimeUnit(): TimeUnit {
        return TimeUnit.MILLISECONDS
    }

    override fun getName(): String {
        return Task.CLEAN_OUT_OLDER_USERS_INITIALIZATON.name
    }

    override fun getDependenciesClass(): Class<out AssemblerDependencies> {
        return AssemblerDependencies::class.java
    }
}