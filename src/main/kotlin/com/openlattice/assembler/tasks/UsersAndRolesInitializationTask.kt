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

import com.hazelcast.core.Hazelcast
import com.openlattice.assembler.*
import com.openlattice.authorization.PrincipalType
import com.openlattice.authorization.SecurablePrincipal
import com.openlattice.authorization.initializers.AuthorizationBootstrap
import com.openlattice.organization.roles.Role
import com.openlattice.organizations.OrganizationBootstrap
import com.openlattice.organizations.roles.SecurePrincipalsManager
import com.openlattice.postgres.ResultSetAdapters
import com.openlattice.postgres.streams.PostgresIterable
import com.openlattice.postgres.streams.StatementHolder
import com.openlattice.tasks.HazelcastInitializationTask
import com.openlattice.tasks.Task
import java.util.concurrent.TimeUnit

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class UsersAndRolesInitializationTask : HazelcastInitializationTask<AssemblerDependencies> {
    override fun initialize(dependencies: AssemblerDependencies) {
        dependencies
                .assemblerConnectionManager
                .getAllRoles(dependencies.securePrincipalsManager)
                .map(dependencies.assemblerConnectionManager::createRole)
        dependencies
                .assemblerConnectionManager
                .getAllUsers(dependencies.securePrincipalsManager)
                .map(dependencies.assemblerConnectionManager::createUnprivilegedUser)

    }

    override fun after(): Set<Class<out HazelcastInitializationTask<*>>> {
        return setOf(OrganizationBootstrap::class.java, AuthorizationBootstrap::class.java)
    }

    override fun getInitialDelay(): Long {
        return 0
    }

    override fun getTimeUnit(): TimeUnit {
        return TimeUnit.MILLISECONDS
    }

    override fun getName(): String {
        return Task.USERS_AND_ROLES_INITIALIZATON.name
    }

    override fun getDependenciesClass(): Class<out AssemblerDependencies> {
        return AssemblerDependencies::class.java
    }


}