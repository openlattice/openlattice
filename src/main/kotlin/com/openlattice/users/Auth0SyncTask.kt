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
import com.openlattice.authorization.mapstores.UserMapstore
import com.openlattice.bootstrap.AuthorizationBootstrap
import com.openlattice.client.RetrofitFactory
import com.openlattice.client.serialization.SerializationConstants
import com.openlattice.datastore.services.Auth0ManagementApi
import com.openlattice.directory.pojo.Auth0UserBasic
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.organization.OrganizationConstants.Companion.GLOBAL_ORG_PRINCIPAL
import com.openlattice.organization.OrganizationConstants.Companion.OPENLATTICE_ORG_PRINCIPAL
import org.slf4j.LoggerFactory
import retrofit2.Retrofit
import java.time.OffsetDateTime
import java.time.temporal.ChronoUnit
import java.util.*

const val REFRESH_INTERVAL_MILLIS = 30000L
private const val DEFAULT_PAGE_SIZE = 100
private val logger = LoggerFactory.getLogger(Auth0SyncTask::class.java)


/**
 * This is the auth0 synchronization task that runs every REFRESH_INTERVAL_MILLIS in Hazelcast. It requires that
 * Auth0SyncHelpers be initialized within the same JVM in order to function properly.
 *
 */
class Auth0SyncTask(

) : Runnable {

    override fun run() {
        if (!Auth0SyncHelpers.initialized) return
        val users: IMap<String, Auth0UserBasic> = Auth0SyncHelpers.hazelcastInstance.getMap(HazelcastMap.USERS.name)
        val retrofit: Retrofit = RetrofitFactory.newClient(
                Auth0SyncHelpers.auth0TokenProvider.managementApiUrl
        ) { Auth0SyncHelpers.auth0TokenProvider.token }
        val auth0ManagementApi = retrofit.create(Auth0ManagementApi::class.java)
        val userRoleAclKey: AclKey = Auth0SyncHelpers.spm.lookup(AuthorizationBootstrap.GLOBAL_USER_ROLE.principal)
        val adminRoleAclKey: AclKey = Auth0SyncHelpers.spm.lookup(AuthorizationBootstrap.GLOBAL_ADMIN_ROLE.principal)

        val globalOrganizationAclKey: AclKey = Auth0SyncHelpers.spm.lookup(
                GLOBAL_ORG_PRINCIPAL
        )
        val openlatticeOrganizationAclKey: AclKey = Auth0SyncHelpers.spm.lookup(
                OPENLATTICE_ORG_PRINCIPAL
        )
        //Only one instance can populate and refresh the map. Unforunately, ILock is refusing to unlock causing issues
        //So we implement a different gating mechanism. This may occasionally be wrong when cluster size changes.
        logger.info("Refreshing user list from Auth0.")
        try {
            var page = 0
            var pageOfUsers: Set<Auth0UserBasic>? = auth0ManagementApi.getAllUsers(page++, DEFAULT_PAGE_SIZE)
            while (pageOfUsers != null && !pageOfUsers.isEmpty()) {
                logger.info("Loading page {} of {} auth0 users", page, pageOfUsers.size)
                pageOfUsers
                        .parallelStream()
                        .forEach { user ->
                            val userId = user.userId
                            users.set(userId, user)
                            if (Auth0SyncHelpers.dbCredentialService.createUserIfNotExists(userId) != null) {
                                createPrincipal(
                                        user, userId, globalOrganizationAclKey, userRoleAclKey,
                                        openlatticeOrganizationAclKey, adminRoleAclKey
                                )
                            } else if (user.roles.contains(SystemRole.ADMIN.getName())) {
                                val principal = Auth0SyncHelpers.spm.getPrincipal(userId)
                                if (principal != null && !Auth0SyncHelpers.spm.principalHasChildPrincipal(principal.aclKey, adminRoleAclKey)) {
                                    Auth0SyncHelpers.organizationService
                                            .addRoleToPrincipalInOrganization(adminRoleAclKey[0], adminRoleAclKey[1], principal.principal)
                                }

                            }
                        }
                pageOfUsers = auth0ManagementApi.getAllUsers(page++, DEFAULT_PAGE_SIZE)
            }
        } finally {
            /*
             * If we did not see a user in any of the pages we should delete that user.
             * In the future we should consider persisting users to our own database so that we
             * don't have to load all of them at startup.
             */
            users.removeAll(
                    Predicates.lessThan(
                            UserMapstore.LOAD_TIME_INDEX,
                            OffsetDateTime.now().minus(6*REFRESH_INTERVAL_MILLIS, ChronoUnit.SECONDS)
                    ) as Predicate<String, Auth0UserBasic>?
            )
        }
    }

    private fun createPrincipal(
            user: Auth0UserBasic, userId: String, globalOrganizationAclKey: AclKey, userRoleAclKey: AclKey,
            openlatticeOrganizationAclKey: AclKey, adminRoleAclKey: AclKey
    ) {
        val principal = Principal(PrincipalType.USER, userId)
        val title = if (user.nickname != null && user.nickname.isNotEmpty())
            user.nickname
        else
            user.email

        Auth0SyncHelpers.spm.createSecurablePrincipalIfNotExists(
                principal,
                SecurablePrincipal(Optional.empty(), principal, title, Optional.empty())
        )

        if (user.roles.contains(SystemRole.AUTHENTICATED_USER.getName())) {
            Auth0SyncHelpers.organizationService.addMembers(globalOrganizationAclKey, ImmutableSet.of(principal))
            Auth0SyncHelpers.organizationService
                    .addRoleToPrincipalInOrganization(userRoleAclKey[0], userRoleAclKey[1], principal)
        }

        if (user.roles.contains(SystemRole.ADMIN.getName())) {
            Auth0SyncHelpers.organizationService.addMembers(openlatticeOrganizationAclKey, ImmutableSet.of(principal))
            Auth0SyncHelpers.organizationService
                    .addRoleToPrincipalInOrganization(adminRoleAclKey[0], adminRoleAclKey[1], principal)
        }
    }
}
