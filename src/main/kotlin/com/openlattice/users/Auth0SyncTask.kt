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
import com.openlattice.organization.OrganizationConstants.Companion.OPENLATTICE_ORG_PRINCIPAL
import com.openlattice.organizations.PrincipalSet
import com.openlattice.postgres.mapstores.OrganizationMembersMapstore
import com.openlattice.retrofit.RhizomeRetrofitCallException
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

        val membersOf: IMap<UUID, PrincipalSet> = ds.hazelcastInstance.getMap(HazelcastMap.ORGANIZATIONS_MEMBERS.name)

        val users: IMap<String, Auth0UserBasic> = ds.hazelcastInstance.getMap(HazelcastMap.USERS.name)
        val retrofit: Retrofit = RetrofitFactory.newClient(
                ds.auth0TokenProvider.managementApiUrl
        ) { ds.auth0TokenProvider.token }
        val auth0ManagementApi = retrofit.create(Auth0ManagementApi::class.java)
        val userRoleAclKey: AclKey = ds.spm.lookup(AuthorizationInitializationTask.GLOBAL_USER_ROLE.principal)
        val adminRoleAclKey: AclKey = ds.spm.lookup(AuthorizationInitializationTask.GLOBAL_ADMIN_ROLE.principal)

        val globalOrganizationAclKey: AclKey = ds.spm.lookup(
                GLOBAL_ORG_PRINCIPAL
        )
        val openlatticeOrganizationAclKey: AclKey = ds.spm.lookup(
                OPENLATTICE_ORG_PRINCIPAL
        )
        //Only one instance can populate and refresh the map. Unforunately, ILock is refusing to unlock causing issues
        //So we implement a different gating mechanism. This may occasionally be wrong when cluster size changes.
        logger.info("Refreshing user list from Auth0.")
        try {
            var page = 0
            var pageOfUsers: Set<Auth0UserBasic> = auth0ManagementApi.getAllUsers(page++, DEFAULT_PAGE_SIZE)!!
            check(pageOfUsers.isNotEmpty() || users.isNotEmpty()) { "No users found." }
            while (pageOfUsers.isNotEmpty()) {
                logger.info("Loading page {} of {} auth0 users", page, pageOfUsers.size)
                pageOfUsers
                        .parallelStream()
                        .forEach { user ->
                            val userId = user.userId
                            users.set(userId, user)

                            if (!ds.spm.principalExists(Principal(PrincipalType.USER, userId))) {
                                createPrincipal(
                                        user,
                                        userId,
                                        globalOrganizationAclKey,
                                        userRoleAclKey,
                                        openlatticeOrganizationAclKey,
                                        adminRoleAclKey,
                                        ds
                                )
                            }

                            //If the user is an admin but doesn't have admin permissions grant him correct permissions.
                            if (user.roles.contains(SystemRole.ADMIN.getName())) {
                                val principal = ds.spm.getPrincipal(userId)
                                if (!ds.spm.principalHasChildPrincipal(principal.aclKey, adminRoleAclKey)) {
                                    ds.organizationService
                                            .addRoleToPrincipalInOrganization(
                                                    adminRoleAclKey[0],
                                                    adminRoleAclKey[1],
                                                    principal.principal
                                            )
                                }
                            }
                        }
                pageOfUsers = auth0ManagementApi.getAllUsers(page++, DEFAULT_PAGE_SIZE)
            }
        } catch (ex: Exception) {
            logger.error("Retrofit called failed during auth0 sync task.", ex)
            return
        }

        val removeUsersPredicate = Predicates.lessThan(
                UserMapstore.LOAD_TIME_INDEX,
                OffsetDateTime.now().minus(6 * REFRESH_INTERVAL_MILLIS, ChronoUnit.MILLIS).toInstant().toEpochMilli()
        ) as Predicate<String, Auth0UserBasic>?

        val usersToRemove = users.keySet(removeUsersPredicate)
        logger.info("Removing the following users: {}", usersToRemove)

        /*
         *  Need to remove members from their respective orgs here
         */
        membersOf.executeOnEntries(RemoveMemberOfOrganizationEntryProcessor(usersToRemove))

        /*
         * If we did not see a user in any of the pages we should delete that user.
         * In the future we should consider persisting users to our own database so that we
         * don't have to load all of them at startup.
         */
        users.removeAll(removeUsersPredicate)

    }

    private fun createPrincipal(
            user: Auth0UserBasic, userId: String,
            globalOrganizationAclKey: AclKey,
            userRoleAclKey: AclKey,
            openlatticeOrganizationAclKey: AclKey,
            adminRoleAclKey: AclKey,
            syncDependencies: Auth0SyncTaskDependencies
    ) {
        val principal = Principal(PrincipalType.USER, userId)
        val title = if (user.nickname != null && user.nickname.isNotEmpty())
            user.nickname
        else
            user.email

        syncDependencies.spm.createSecurablePrincipalIfNotExists(
                principal,
                SecurablePrincipal(Optional.empty(), principal, title, Optional.empty())
        )

        if (user.roles.contains(SystemRole.AUTHENTICATED_USER.getName())) {
            syncDependencies.organizationService.addMembers(globalOrganizationAclKey[0], ImmutableSet.of(principal))
            syncDependencies.organizationService
                    .addRoleToPrincipalInOrganization(userRoleAclKey[0], userRoleAclKey[1], principal)
        }

        if (user.roles.contains(SystemRole.ADMIN.getName())) {
            syncDependencies.organizationService.addMembers(
                    openlatticeOrganizationAclKey[0], ImmutableSet.of(principal)
            )
            syncDependencies.organizationService
                    .addRoleToPrincipalInOrganization(adminRoleAclKey[0], adminRoleAclKey[1], principal)
        }
    }
}
