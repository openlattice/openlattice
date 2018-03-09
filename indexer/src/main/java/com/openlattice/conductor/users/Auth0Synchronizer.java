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

package com.openlattice.conductor.users;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.openlattice.auth0.Auth0TokenProvider.AUTH0_MANAGEMENT_API_V2_URL;

import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Optional;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IMap;
import com.hazelcast.core.IQueue;
import com.openlattice.auth0.Auth0TokenProvider;
import com.openlattice.authorization.AclKey;
import com.openlattice.authorization.DbCredentialService;
import com.openlattice.authorization.Principal;
import com.openlattice.authorization.PrincipalType;
import com.openlattice.authorization.SecurablePrincipal;
import com.openlattice.authorization.SystemRole;
import com.openlattice.authorization.mapstores.UserMapstore;
import com.openlattice.bootstrap.AuthorizationBootstrap;
import com.openlattice.client.RetrofitFactory;
import com.openlattice.datastore.services.Auth0ManagementApi;
import com.openlattice.directory.pojo.Auth0UserBasic;
import com.openlattice.hazelcast.HazelcastMap;
import com.openlattice.organizations.roles.SecurePrincipalsManager;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import retrofit2.Retrofit;

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
    private final AclKey                       userRoleAclKey;
    private final AclKey                       adminRoleAclKey;

    public Auth0Synchronizer(
            HazelcastInstance hazelcastInstance,
            SecurePrincipalsManager spm,
            DbCredentialService dbCredentialService,
            Auth0TokenProvider auth0TokenProvider
    ) {
        this.users = hazelcastInstance.getMap( HazelcastMap.USERS.name() );
        this.memberIds = hazelcastInstance.getQueue( Auth0Synchronizer.class.getCanonicalName() );
        this.nextTime = hazelcastInstance.getAtomicLong( UserMapstore.class.getCanonicalName() );
        this.retrofit = RetrofitFactory.newClient( AUTH0_MANAGEMENT_API_V2_URL, auth0TokenProvider::getToken );
        this.auth0ManagementApi = retrofit.create( Auth0ManagementApi.class );
        this.hazelcastInstance = hazelcastInstance;
        this.localMemberId = checkNotNull( hazelcastInstance.getLocalEndpoint().getUuid() );
        this.dbCredentialService = dbCredentialService;
        this.spm = spm;
        this.userRoleAclKey = spm.lookup( AuthorizationBootstrap.GLOBAL_USER_ROLE.getPrincipal() );
        this.adminRoleAclKey = spm.lookup( AuthorizationBootstrap.GLOBAL_ADMIN_ROLE.getPrincipal() );
        memberIds.add( localMemberId );
    }

    @Timed
    void refreshAuth0Users() {
        //Only one instance can populate and refresh the map. Unforunately, ILock is refusing to unlock causing issues
        //So we implement a different gating mechanism. This may occasionally be wrong when cluster size changes.
        logger.info( "Trying to acquire lock to refresh auth0 users." );
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
                                users.set( userId, user, -1, TimeUnit.MINUTES );
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
            }
        } else {
            logger.info( "Not elected to refresh users." );
            //Ensure that every active member is registered for selection.
            memberIds.add( localMemberId );
        }


    }

    public void createPrincipal( Auth0UserBasic user, String userId ) {
        final Principal principal = new Principal( PrincipalType.USER, userId );
        final String title = ( user.getNickname() != null && user.getNickname().length() > 0 ) ?
                user.getNickname() :
                user.getEmail();

        spm.createSecurablePrincipalIfNotExists( principal,
                new SecurablePrincipal( Optional.absent(), principal, title, Optional.absent() ) );

        AclKey userAclKey = spm.lookup( principal );

        if ( user.getRoles().contains( SystemRole.AUTHENTICATED_USER.getName() ) ) {
            spm.addPrincipalToPrincipal( userRoleAclKey, userAclKey );
        }

        if ( user.getRoles().contains( SystemRole.ADMIN.getName() ) ) {
            spm.addPrincipalToPrincipal( adminRoleAclKey, userAclKey );
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
