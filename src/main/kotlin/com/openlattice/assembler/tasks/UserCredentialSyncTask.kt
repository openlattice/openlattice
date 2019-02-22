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

import com.openlattice.assembler.AssemblerDependencies
import com.openlattice.assembler.PostgresRoles.Companion.buildPostgresUsername
import com.openlattice.postgres.DataTables
import com.openlattice.tasks.HazelcastInitializationTask
import com.openlattice.tasks.PostConstructInitializerTaskDependencies.PostConstructInitializerTask
import com.openlattice.tasks.Task
import org.slf4j.LoggerFactory
import java.util.concurrent.TimeUnit


private val logger = LoggerFactory.getLogger(UserCredentialSyncTask::class.java)

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class UserCredentialSyncTask : HazelcastInitializationTask<AssemblerDependencies> {
    override fun initialize(dependencies: AssemblerDependencies) {
        dependencies
                .assemblerConnectionManager
                .getAllUsers(dependencies.securePrincipalsManager)
                .forEach { user ->
                    dependencies.target.connection.use { conn ->
                        conn.createStatement().use { stmt ->
                            val username = buildPostgresUsername(user)
                            val credential = dependencies.dbCredentialService.getOrCreateUserCredentials(username)
                            try {
                                stmt.execute(
                                        "ALTER USER ${DataTables.quote(username)} WITH ENCRYPTED PASSWORD '$credential'"
                                )
                            } catch (ex: Exception) {
                                logger.error("Unable to set credential for user {}", username, ex)
                            }
                        }
                    }
                }
    }

    override fun after(): Set<Class<out HazelcastInitializationTask<*>>> {
        return setOf(PostConstructInitializerTask::class.java)
    }

    override fun getInitialDelay(): Long {
        return 0
    }

    override fun getTimeUnit(): TimeUnit {
        return TimeUnit.MILLISECONDS
    }

    override fun getName(): String {
        return Task.USER_CREDENTIAL_SYNC_TASK.name
    }

    override fun getDependenciesClass(): Class<out AssemblerDependencies> {
        return AssemblerDependencies::class.java
    }

    override fun isRunOnceAcrossCluster(): Boolean {
        return false
    }
}