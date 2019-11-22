/*
 * Copyright (C) 2018. OpenLattice, Inc.
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


import com.auth0.client.auth.AuthAPI
import com.auth0.client.mgmt.ManagementAPI
import com.auth0.client.mgmt.filter.UserFilter
import com.auth0.json.mgmt.users.User
import com.auth0.json.mgmt.users.UsersPage
import com.google.common.collect.ImmutableSet
import com.hazelcast.core.IMap
import com.hazelcast.query.Predicate
import com.hazelcast.query.Predicates
import com.openlattice.authorization.*
import com.openlattice.authorization.initializers.AuthorizationInitializationTask
import com.openlattice.authorization.mapstores.UserMapstore
import com.openlattice.client.RetrofitFactory
import com.openlattice.datastore.services.Auth0ManagementApi
import com.openlattice.directory.pojo.Auth0UserBasic
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.hazelcast.processors.RemoveMemberOfOrganizationEntryProcessor
import com.openlattice.organization.OrganizationConstants.Companion.GLOBAL_ORG_PRINCIPAL
import com.openlattice.organizations.PrincipalSet
import com.openlattice.tasks.HazelcastFixedRateTask
import com.openlattice.tasks.HazelcastTaskDependencies
import com.openlattice.tasks.Task
import org.slf4j.LoggerFactory
import retrofit2.Retrofit
import java.time.OffsetDateTime
import java.time.temporal.ChronoUnit
import java.util.*
import java.util.concurrent.TimeUnit

const val REFRESH_INTERVAL_MILLIS = 30000L
private const val DEFAULT_PAGE_SIZE = 100
private val logger = LoggerFactory.getLogger(Auth0SyncTask::class.java)


/**
 * This is the auth0 synchronization task that runs every REFRESH_INTERVAL_MILLIS in Hazelcast. It requires that
 * syncDependencies be initialized within the same JVM in order to function properly.
 *
 */
class Auth0SyncTask : HazelcastFixedRateTask<Auth0SyncTaskDependencies>, HazelcastTaskDependencies {
    override fun getDependenciesClass(): Class<Auth0SyncTaskDependencies> {
        return Auth0SyncTaskDependencies::class.java
    }

    override fun getName(): String {
        return Task.AUTH0_SYNC_TASK.name
    }

    override fun getInitialDelay(): Long {
        return REFRESH_INTERVAL_MILLIS
    }

    override fun getPeriod(): Long {
        return REFRESH_INTERVAL_MILLIS
    }

    override fun getTimeUnit(): TimeUnit {
        return TimeUnit.MILLISECONDS
    }

    override fun runTask() {
        val ds = getDependency()

        logger.info("Refreshing user list from Auth0.")
        try {
            var page = 0
            var pageOfUsers = getUsersPage(ds.managementApi, page++).items
            require(pageOfUsers.isNotEmpty()) { "No users found." }
            while (pageOfUsers.isNotEmpty()) {
                logger.info("Loading page {} of {} auth0 users", page, pageOfUsers.size)
                pageOfUsers
                        .parallelStream()
                        .forEach(ds.users::syncUser)


                pageOfUsers = getUsersPage(ds.managementApi, page++).items
            }
        } catch (ex: Exception) {
            logger.error("Retrofit called failed during auth0 sync task.", ex)
            return
        }

        /*
         * If we did not see a user in any of the pages we should delete that user from the users table and all
         * organizations.
         */

        val usersToRemove = ds.users.getExpiredUsers()
        logger.info("Removing the following users: {}", usersToRemove)
        usersToRemove
                .map(::getPrincipal)
                .forEach { principal ->
                    ds.organizationService.removeUser(principal)
                    ds.users.remove(principal.id)
                }


    }

    private fun getUsersPage(managementApi: ManagementAPI, page: Int, pageSize: Int = DEFAULT_PAGE_SIZE): UsersPage {
        return managementApi.users().list(
                UserFilter()
                        .withSearchEngine("v3")
                        .withFields("user_id,email,nickname,app_metadata,identities",true)
                        .withPage(page, pageSize)
        ).execute()
    }

}

