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

package com.openlattice.users

import com.openlattice.assembler.tasks.UsersAndRolesInitializationTask
import com.openlattice.organizations.tasks.OrganizationsInitializationTask
import com.openlattice.tasks.HazelcastInitializationTask
import com.openlattice.tasks.PostConstructInitializerTaskDependencies
import com.openlattice.tasks.Task.AUTH0_SYNC_INITIALIZATION_TASK

class Auth0SyncInitializationTask : HazelcastInitializationTask<Auth0SyncTask> {
    override fun getInitialDelay(): Long {
        return 0L
    }

    override fun getName(): String {
        return AUTH0_SYNC_INITIALIZATION_TASK.name
    }

    override fun getDependenciesClass(): Class<out Auth0SyncTask> {
        return Auth0SyncTask::class.java
    }

    override fun initialize(dependencies: Auth0SyncTask) {
        dependencies.run()
    }

    override fun after(): Set<Class<out HazelcastInitializationTask<*>>> {
        return setOf(
                PostConstructInitializerTaskDependencies.PostConstructInitializerTask::class.java,
                UsersAndRolesInitializationTask::class.java,
                OrganizationsInitializationTask::class.java
        )
    }

}