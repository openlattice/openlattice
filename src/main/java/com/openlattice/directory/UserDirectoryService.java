

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
 */

package com.openlattice.directory;

import com.codahale.metrics.annotation.Timed;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.openlattice.auth0.Auth0TokenProvider;
import com.openlattice.authentication.Auth0Configuration;
import com.openlattice.client.RetrofitFactory;
import com.openlattice.datastore.services.Auth0ManagementApi;
import com.openlattice.directory.pojo.Auth0UserBasic;
import com.openlattice.hazelcast.HazelcastMap;
import com.openlattice.organization.roles.Role;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import retrofit2.Retrofit;

public class UserDirectoryService {
    private static final Logger logger            = LoggerFactory.getLogger( UserDirectoryService.class );
    private static final int    DEFAULT_PAGE_SIZE = 100;
    //TODO: Switch over to a Hazelcast map to relieve pressure from Auth0
    @SuppressWarnings( "unused" )
    private final IMap<String, Auth0UserBasic> users;
    private       Retrofit                     retrofit;
    private       Auth0ManagementApi           auth0ManagementApi;

    public UserDirectoryService(
            Auth0TokenProvider auth0TokenProvider,
            HazelcastInstance hazelcastInstance
    ) {
        retrofit = RetrofitFactory.newClient( auth0TokenProvider.getManagementApiUrl(), auth0TokenProvider::getToken );
        auth0ManagementApi = retrofit.create( Auth0ManagementApi.class );
        users = hazelcastInstance.getMap( HazelcastMap.USERS.name() );
    }

    @Timed
    public Map<String, Auth0UserBasic> getAllUsers() {
        return ImmutableMap.copyOf( users );
        //        return users
        //                .entrySet()
        //                .stream().filter( e-> !(e.getValue()==null ))
        //                .collect( Collectors.toConcurrentMap( Entry::getAclKey, Entry::getValue ) );
    }

    @Timed
    public Auth0UserBasic getUser( String userId ) {
        return users.get( userId );
    }

    public void setRolesOfUser( String userId, Set<String> roleList ) {
        auth0ManagementApi.resetRolesOfUser( userId,
                ImmutableMap.of( "app_metadata", ImmutableMap.of( "roles", roleList ) ) );
    }

    public void addRoleToUser( String userId, String role ) {

        Auth0UserBasic user = getUser( userId );

        if ( user != null ) {
            Set<String> roles = new HashSet<>( user.getRoles() );
            roles.add( role );
            setRolesOfUser( userId, roles );
        } else {
            logger.warn( "Received null user from Auth0" );
        }
    }

    public void removeRoleFromUser( String userId, String role ) {

        Auth0UserBasic user = getUser( userId );

        if ( user != null ) {
            Set<String> roles = new HashSet<>( user.getRoles() );
            roles.remove( role );
            setRolesOfUser( userId, roles );
        } else {
            logger.warn( "Received null user from Auth0" );
        }
    }

    public void updateRoleOfUser( String userId, String roleToRemove, String roleToAdd ) {

        Auth0UserBasic user = getUser( userId );

        if ( user != null ) {
            Set<String> roles = new HashSet<>( user.getRoles() );
            roles.remove( roleToRemove );
            roles.add( roleToAdd );
            setRolesOfUser( userId, roles );
        } else {
            logger.warn( "Received null user from Auth0" );
        }
    }

    public void removeAllRolesInOrganizationFromUser( String userId, List<Role> allRolesInOrg ) {

        Auth0UserBasic user = getUser( userId );

        if ( user != null ) {
            Set<String> roles = new HashSet<>( user.getRoles() );
            for ( Role role : allRolesInOrg ) {
                roles.remove( role.getId().toString() );
            }
            setRolesOfUser( userId, roles );
        } else {
            logger.warn( "Received null user from Auth0" );
        }
    }

    public void removeOrganizationFromUser( String userId, UUID organization ) {

        Auth0UserBasic user = getUser( userId );

        if ( user != null ) {
            Set<String> organizations = new HashSet<>( user.getOrganizations() );
            organizations.remove( organization.toString() );
            setOrganizationsOfUser( userId, organizations );
        } else {
            logger.warn( "Received null user from Auth0" );
        }
    }

    public void setOrganizationsOfUser( String userId, Set<String> organizations ) {
        auth0ManagementApi.resetRolesOfUser( userId,
                ImmutableMap.of( "app_metadata", ImmutableMap.of( "organizations", organizations ) ) );

    }

    public Map<String, Auth0UserBasic> searchAllUsers( String searchQuery ) {

        int page = 0;
        Set<Auth0UserBasic> pageOfUsers;
        Set<Auth0UserBasic> users = Sets.newHashSet();

        do {
            pageOfUsers = auth0ManagementApi.searchAllUsers( searchQuery, page++, DEFAULT_PAGE_SIZE );
            if ( pageOfUsers != null ) {
                users.addAll( pageOfUsers );
            }
        } while ( pageOfUsers != null && pageOfUsers.size() == DEFAULT_PAGE_SIZE );

        if ( users.isEmpty() ) {
            logger.warn( "Auth0 did not return any users for this search." );
            return ImmutableMap.of();
        }

        return users
                .stream()
                .collect( Collectors.toMap( Auth0UserBasic::getUserId, Function.identity() ) );
    }
}
