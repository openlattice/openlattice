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

import com.openlattice.assembler.UserRoleSyncTaskDependencies
import com.openlattice.authorization.initializers.AuthorizationInitializationTask
import com.geekbeast.tasks.HazelcastInitializationTask
import com.geekbeast.tasks.Task
import java.util.concurrent.TimeUnit

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class UsersAndRolesInitializationTask : HazelcastInitializationTask<UserRoleSyncTaskDependencies> {
    override fun initialize(dependencies: UserRoleSyncTaskDependencies) {
        dependencies.securePrincipalsManager.allRoles
                .map(dependencies.extDbPermissioner::createRole)
        dependencies.securePrincipalsManager.allUsers
                .map(dependencies.extDbPermissioner::createUnprivilegedUser)
    }

    override fun after(): Set<Class<out HazelcastInitializationTask<*>>> {
        return setOf(AuthorizationInitializationTask::class.java)
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

    override fun getDependenciesClass(): Class<out UserRoleSyncTaskDependencies> {
        return UserRoleSyncTaskDependencies::class.java
    }
}