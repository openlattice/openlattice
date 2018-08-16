/*
 * Copyright (C) 2017. OpenLattice, Inc
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
 */

package com.openlattice.users;

import com.codahale.metrics.annotation.Timed;
import com.google.common.collect.ImmutableSet;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IMap;
import com.hazelcast.core.IQueue;
import com.hazelcast.query.Predicates;
import com.openlattice.auth0.Auth0TokenProvider;
import com.openlattice.authorization.*;
import com.openlattice.authorization.mapstores.UserMapstore;
import com.openlattice.bootstrap.AuthorizationBootstrap;
import com.openlattice.bootstrap.OrganizationBootstrap;
import com.openlattice.client.RetrofitFactory;
import com.openlattice.datastore.services.Auth0ManagementApi;
import com.openlattice.directory.pojo.Auth0UserBasic;
import com.openlattice.hazelcast.HazelcastMap;
import com.openlattice.organizations.HazelcastOrganizationService;
import com.openlattice.organizations.roles.SecurePrincipalsManager;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import retrofit2.Retrofit;

import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class Auth0Synchronizer {
    public static final  int    REFRESH_INTERVAL_MILLIS = 30000;
    private static final Logger logger                  = LoggerFactory.getLogger( Auth0Synchronizer.class );
    private static final int    DEFAULT_PAGE_SIZE       = 100;

    private final HazelcastInstance            hazelcastInstance;
    private final IMap<String, Auth0UserBasic> users;
    private final Retrofit                     retrofit;
    private final Auth0ManagementApi           auth0ManagementApi;
    private final IQueue<String>               memberIds;
    private final IAtomicLong                  nextTime;
    private final String                       localMemberId;
    private final DbCredentialService          dbCredentialService;
    private final SecurePrincipalsManager      spm;
    private final HazelcastOrganizationService organizationService;
    private final AclKey                       userRoleAclKey;
    private final AclKey                       adminRoleAclKey;
    private final AclKey                       globalOrganizationAclKey;
    private final AclKey                       openlatticeOrganizationAclKey;

    public Auth0Synchronizer(
            HazelcastInstance hazelcastInstance,
            SecurePrincipalsManager spm,
            HazelcastOrganizationService organizationService,
            DbCredentialService dbCredentialService,
            Auth0TokenProvider auth0TokenProvider
    ) {
        this.users = hazelcastInstance.getMap( HazelcastMap.USERS.name() );
        this.memberIds = hazelcastInstance.getQueue( Auth0Synchronizer.class.getCanonicalName() );
        this.nextTime = hazelcastInstance.getAtomicLong( UserMapstore.class.getCanonicalName() );
        this.retrofit = RetrofitFactory
                .newClient( auth0TokenProvider.getManagementApiUrl(), auth0TokenProvider::getToken );
        this.auth0ManagementApi = retrofit.create( Auth0ManagementApi.class );
        this.hazelcastInstance = hazelcastInstance;
        this.localMemberId = checkNotNull( hazelcastInstance.getLocalEndpoint().getUuid() );
        this.dbCredentialService = dbCredentialService;
        this.spm = spm;
        this.organizationService = organizationService;
        this.globalOrganizationAclKey = spm.lookup( OrganizationBootstrap.GLOBAL_ORG_PRINCIPAL );
        this.openlatticeOrganizationAclKey = spm.lookup( OrganizationBootstrap.OPENLATTICE_ORG_PRINCIPAL );
        this.userRoleAclKey = spm.lookup( AuthorizationBootstrap.GLOBAL_USER_ROLE.getPrincipal() );
        this.adminRoleAclKey = spm.lookup( AuthorizationBootstrap.GLOBAL_ADMIN_ROLE.getPrincipal() );
        memberIds.add( localMemberId );
    }

    @Timed
    void refreshAuth0Users() {
        //Only one instance can populate and refresh the map. Unforunately, ILock is refusing to unlock causing issues
        //So we implement a different gating mechanism. This may occasionally be wrong when cluster size changes.
        logger.info( "Trying to acquire lock to refresh auth0 users." );
        logger.info( "\"memberIds\" queue size: {}", memberIds.size() );
        String nextMember = memberIds.peek();
        if ( StringUtils.equals( nextMember, localMemberId ) && ( nextTime.get() < System.currentTimeMillis() ) ) {
            memberIds.poll();
            logger.info( "Refreshing user list from Auth0." );
            try {
                int page = 0;
                Set<Auth0UserBasic> pageOfUsers = auth0ManagementApi.getAllUsers( page++, DEFAULT_PAGE_SIZE );
                while ( pageOfUsers != null && !pageOfUsers.isEmpty() ) {
                    logger.info( "Loading page {} of {} auth0 users", page, pageOfUsers.size() );
                    pageOfUsers
                            .parallelStream()
                            .forEach( user -> {
                                String userId = user.getUserId();
                                users.set( userId, user );
                                if ( dbCredentialService.createUserIfNotExists( userId ) != null ) {
                                    createPrincipal( user, userId );
                                }
                            } );
                    pageOfUsers = auth0ManagementApi.getAllUsers( page++, DEFAULT_PAGE_SIZE );
                }
            } finally {
                logger.info( "Scheduling next refresh." );
                memberIds.add( localMemberId );
                nextTime.set( System.currentTimeMillis() + REFRESH_INTERVAL_MILLIS );
                /*
                 * If we did not see a user in any of the pages we should delete that user.
                 * In the future we should consider persisting users to our own database so that we
                 * don't have to load all of them at startup.
                 */
                users.removeAll(
                        Predicates.lessThan( UserMapstore.LOAD_TIME_INDEX,
                                OffsetDateTime.now().minus( REFRESH_INTERVAL_MILLIS, ChronoUnit.SECONDS ) ) );
            }
        } else {
            logger.info( "Not elected to refresh users." );
        }

    }

    public void createPrincipal( Auth0UserBasic user, String userId ) {
        final Principal principal = new Principal( PrincipalType.USER, userId );
        final String title = ( user.getNickname() != null && user.getNickname().length() > 0 ) ?
                user.getNickname() :
                user.getEmail();

        spm.createSecurablePrincipalIfNotExists( principal,
                new SecurablePrincipal( Optional.empty(), principal, title, Optional.empty() ) );

        if ( user.getRoles().contains( SystemRole.AUTHENTICATED_USER.getName() ) ) {
            organizationService.addMembers( globalOrganizationAclKey, ImmutableSet.of( principal ) );
            organizationService
                    .addRoleToPrincipalInOrganization( userRoleAclKey.get( 0 ), userRoleAclKey.get( 1 ), principal );
        }

        if ( user.getRoles().contains( SystemRole.ADMIN.getName() ) ) {
            organizationService.addMembers( openlatticeOrganizationAclKey, ImmutableSet.of( principal ) );
            organizationService
                    .addRoleToPrincipalInOrganization( adminRoleAclKey.get( 0 ), adminRoleAclKey.get( 1 ), principal );
        }
    }

    public static class Auth0SyncDriver {
        private final Auth0Synchronizer refresher;

        public Auth0SyncDriver( Auth0Synchronizer refresher ) {
            this.refresher = refresher;
        }

        @Scheduled( fixedRate = REFRESH_INTERVAL_MILLIS )
        void refreshAuth0Users() {
            refresher.refreshAuth0Users();
        }
    }
}
