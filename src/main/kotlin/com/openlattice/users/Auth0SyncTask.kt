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
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.core.IAtomicLong
import com.hazelcast.core.IMap
import com.hazelcast.core.IQueue
import com.hazelcast.query.Predicate
import com.hazelcast.query.Predicates
import com.openlattice.auth0.Auth0TokenProvider
import com.openlattice.authorization.*
import com.openlattice.authorization.mapstores.UserMapstore
import com.openlattice.bootstrap.AuthorizationBootstrap
import com.openlattice.bootstrap.OrganizationBootstrap
import com.openlattice.client.RetrofitFactory
import com.openlattice.datastore.services.Auth0ManagementApi
import com.openlattice.directory.pojo.Auth0UserBasic
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.organizations.HazelcastOrganizationService
import com.openlattice.organizations.roles.SecurePrincipalsManager
import org.slf4j.LoggerFactory
import retrofit2.Retrofit
import java.time.OffsetDateTime
import java.time.temporal.ChronoUnit
import java.util.*

const val REFRESH_INTERVAL_MILLIS = 30000L
private const val DEFAULT_PAGE_SIZE = 100
private val logger = LoggerFactory.getLogger(Auth0SyncTask::class.java)


/**
 *
 */
class Auth0SyncTask(

) : Runnable {
    companion object {
        @JvmStatic
        fun setHazelcastInstance(hazelcastInstance: HazelcastInstance) {
            if (!this::hazelcastInstance.isInitialized) {
                this.hazelcastInstance = hazelcastInstance
            }
        }

        @JvmStatic
        fun setPrincipalManager(spm: SecurePrincipalsManager) {
            if (!this::spm.isInitialized) {
                this.spm = spm
            }
        }

        @JvmStatic
        fun setOrganizationService(organizationService: HazelcastOrganizationService) {
            if (!this::organizationService.isInitialized) {
                this.organizationService = organizationService
            }
        }

        @JvmStatic
        fun setDbCredentialService(dbCredentialService: DbCredentialService) {
            if (!this::dbCredentialService.isInitialized) {
                this.dbCredentialService = dbCredentialService
            }
        }

        @JvmStatic
        fun setAuth0TokenProvider(auth0TokenProvider: Auth0TokenProvider) {
            if (!this::auth0TokenProvider.isInitialized) {
                this.auth0TokenProvider = auth0TokenProvider
            }
        }

        private lateinit var hazelcastInstance: HazelcastInstance
        private lateinit var spm: SecurePrincipalsManager
        private lateinit var organizationService: HazelcastOrganizationService
        private lateinit var dbCredentialService: DbCredentialService
        private lateinit var auth0TokenProvider: Auth0TokenProvider
    }

    private val users: IMap<String, Auth0UserBasic> = hazelcastInstance.getMap(HazelcastMap.USERS.name)
    private val retrofit: Retrofit = RetrofitFactory.newClient(
            auth0TokenProvider.managementApiUrl
    ) { auth0TokenProvider.token }
    private val auth0ManagementApi = retrofit.create(Auth0ManagementApi::class.java)
    private val userRoleAclKey: AclKey = spm.lookup(AuthorizationBootstrap.GLOBAL_USER_ROLE.principal)
    private val adminRoleAclKey: AclKey = spm.lookup(AuthorizationBootstrap.GLOBAL_ADMIN_ROLE.principal)

    private val globalOrganizationAclKey: AclKey = spm.lookup(OrganizationBootstrap.GLOBAL_ORG_PRINCIPAL)
    private val openlatticeOrganizationAclKey: AclKey = spm.lookup(OrganizationBootstrap.OPENLATTICE_ORG_PRINCIPAL)


    override fun run() {
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
                            if (dbCredentialService.createUserIfNotExists(userId) != null) {
                                createPrincipal(user, userId)
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
                            OffsetDateTime.now().minus(REFRESH_INTERVAL_MILLIS, ChronoUnit.SECONDS)
                    ) as Predicate<String, Auth0UserBasic>?
            )
        }
    }

    private fun createPrincipal(user: Auth0UserBasic, userId: String) {
        val principal = Principal(PrincipalType.USER, userId)
        val title = if (user.nickname != null && user.nickname.length > 0)
            user.nickname
        else
            user.email

        spm.createSecurablePrincipalIfNotExists(
                principal,
                SecurablePrincipal(Optional.empty(), principal, title, Optional.empty())
        )

        if (user.roles.contains(SystemRole.AUTHENTICATED_USER.getName())) {
            organizationService.addMembers(globalOrganizationAclKey, ImmutableSet.of(principal))
            organizationService
                    .addRoleToPrincipalInOrganization(userRoleAclKey[0], userRoleAclKey[1], principal)
        }

        if (user.roles.contains(SystemRole.ADMIN.getName())) {
            organizationService.addMembers(openlatticeOrganizationAclKey, ImmutableSet.of(principal))
            organizationService
                    .addRoleToPrincipalInOrganization(adminRoleAclKey[0], adminRoleAclKey[1], principal)
        }
    }
}
