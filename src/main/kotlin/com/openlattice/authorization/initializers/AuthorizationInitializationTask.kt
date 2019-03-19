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

package com.openlattice.authorization.initializers

import com.openlattice.authorization.SystemRole
import com.openlattice.organization.OrganizationConstants.Companion.GLOBAL_ORGANIZATION_ID
import com.openlattice.organization.OrganizationConstants.Companion.OPENLATTICE_ORGANIZATION_ID
import com.openlattice.organization.OrganizationConstants.Companion.ROOT_PRINCIPAL_ID
import com.openlattice.organization.roles.Role
import com.openlattice.tasks.HazelcastInitializationTask
import com.openlattice.tasks.Task
import java.util.*
import java.util.concurrent.TimeUnit

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class AuthorizationInitializationTask : HazelcastInitializationTask<AuthorizationInitializationDependencies> {
    override fun initialize(dependencies: AuthorizationInitializationDependencies) {
        val spm = dependencies.securePrincipalsManager
        spm.createSecurablePrincipalIfNotExists(SystemRole.OPENLATTICE.principal, OPENLATTICE_ROLE)
        spm.createSecurablePrincipalIfNotExists(SystemRole.AUTHENTICATED_USER.principal, GLOBAL_USER_ROLE)
        spm.createSecurablePrincipalIfNotExists(SystemRole.ADMIN.principal, GLOBAL_ADMIN_ROLE)
        val source = spm.lookup(SystemRole.AUTHENTICATED_USER.principal)
        val target = spm.lookup(SystemRole.AUTHENTICATED_USER.principal)
        spm.addPrincipalToPrincipal(source, target)
    }

    override fun after(): Set<Class<out HazelcastInitializationTask<*>>> {
        return setOf()
    }

    override fun getInitialDelay(): Long {
        return 0
    }

    override fun getTimeUnit(): TimeUnit {
        return TimeUnit.MILLISECONDS
    }

    override fun getName(): String {
        return Task.AUTHORIZATION_BOOTSTRAP.name
    }

    override fun getDependenciesClass(): Class<out AuthorizationInitializationDependencies> {
        return AuthorizationInitializationDependencies::class.java
    }

    companion object {

        @JvmField
        val OPENLATTICE_ROLE = Role(
                Optional.of(ROOT_PRINCIPAL_ID),
                OPENLATTICE_ORGANIZATION_ID,
                SystemRole.OPENLATTICE.principal,
                "OpenLattice Root Group",
                Optional.of("Initial account granting access to everything.")
        )

        @JvmField
        val GLOBAL_USER_ROLE = Role(
                Optional.empty(),
                GLOBAL_ORGANIZATION_ID,
                SystemRole.AUTHENTICATED_USER.principal,
                "OpenLattice User Role",
                Optional.of("The default user role granted to all authenticated users of the system.")
        )

        @JvmField
        val GLOBAL_ADMIN_ROLE = Role(
                Optional.empty(),
                GLOBAL_ORGANIZATION_ID,
                SystemRole.ADMIN.principal,
                "Global Admin Role",
                Optional.of("The global administrative role that allows management of entity data model.")
        )
    }

}
