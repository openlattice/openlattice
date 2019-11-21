/*
 * Copyright (C) 2018. OpenLattice, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * You can contact the owner of the copyright at support@openlattice.com
 */

package com.openlattice.organization;

import com.openlattice.directory.pojo.Auth0UserBasic;
import com.openlattice.organization.roles.Role;
import com.openlattice.organizations.Grant;
import com.openlattice.organizations.Organization;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import javax.annotation.Nonnull;
import retrofit2.http.Body;
import retrofit2.http.DELETE;
import retrofit2.http.GET;
import retrofit2.http.HTTP;
import retrofit2.http.POST;
import retrofit2.http.PUT;
import retrofit2.http.Path;

public interface OrganizationsApi {
    // @formatter:off

    String ASSEMBLE          = "/assemble";
    String CONNECTIONS       = "/connections";
    String CONTROLLER        = "/organizations";
    String DESCRIPTION       = "/description";
    String EMAIL_DOMAIN      = "email-domain";
    String EMAIL_DOMAINS     = "/email-domains";
    String EMAIL_DOMAIN_PATH = "/{" + EMAIL_DOMAIN + ":.+}";
    String ENTITY_SETS       = "/entity-sets";
    String GRANT             = "/grant";
    String ID                = "id";
    String ID_PATH           = "/{" + ID + "}";
    String INTEGRATION       = "/integration";
    String MEMBERS           = "/members";
    String PRINCIPALS        = "/principals";
    String PRINCIPAL_ID      = "pid";
    String PRINCIPAL_ID_PATH = "/{" + PRINCIPAL_ID + "}";
    String REFRESH           = "/refresh";
    String REFRESH_RATE      = "/refresh-rate";
    String ROLES             = "/roles";
    String ROLE_ID           = "roleId";
    String ROLE_ID_PATH      = "/{" + ROLE_ID + "}";
    String SERVICE           = "/datastore";
    String BASE              = SERVICE + CONTROLLER;
    String SET_ID            = "setId";
    String SET_ID_PATH       = "/{" + SET_ID + "}";
    String SYNCHRONIZE       = "/synchronize";
    String TITLE             = "/title";
    String TYPE              = "type";
    String TYPE_PATH         = "/{" + TYPE + "}";
    String USER_ID           = "userId";
    String USER_ID_PATH      = "/{" + USER_ID + ":.*}";

    // @formatter:on

    @GET( BASE )
    Iterable<Organization> getOrganizations();

    @POST( BASE )
    UUID createOrganizationIfNotExists( @Body Organization organization );

    @GET( BASE + ID_PATH )
    Organization getOrganization( @Path( ID ) UUID organizationId );

    @DELETE( BASE + ID_PATH )
    Void destroyOrganization( @Path( ID ) UUID organizationId );

    @GET( BASE + ID_PATH + INTEGRATION )
    OrganizationIntegrationAccount getOrganizationIntegrationAccount( @Path( ID ) UUID organizationId );

    /**
     * Retrieves all the organization entity sets without filtering.
     *
     * @param organizationId The id of the organization for which to retrieve entity sets
     * @return All the organization entity sets along with flags indicating whether they are internal, external, and/or
     * materialized.
     */
    @GET( BASE + ID_PATH + ENTITY_SETS )
    Map<UUID, Set<OrganizationEntitySetFlag>> getOrganizationEntitySets( @Path( ID ) UUID organizationId );

    /**
     * Retrieves the entity sets belong to an organization matching a specified filter.
     *
     * @param organizationId The id of the organization for which to retrieve entity sets
     * @param flagFilter The set of flags for which to retrieve information.
     * @return All the organization entity sets along with flags indicating whether they are internal, external, and/or
     * materialized.
     */
    @POST( BASE + ID_PATH + ENTITY_SETS )
    Map<UUID, Set<OrganizationEntitySetFlag>> getOrganizationEntitySets(
            @Path( ID ) UUID organizationId,
            @Body EnumSet<OrganizationEntitySetFlag> flagFilter );

    /**
     * Materializes entity sets into the organization database.
     *
     * @param refreshRatesOfEntitySets The refresh rate in minutes of each entity set to assemble into materialized
     * views mapped by their ids.
     */
    @POST( BASE + ID_PATH + ENTITY_SETS + ASSEMBLE )
    Map<UUID, Set<OrganizationEntitySetFlag>> assembleEntitySets(
            @Path( ID ) UUID organizationId,
            @Body Map<UUID, Integer> refreshRatesOfEntitySets );

    /**
     * Synchronizes EDM changes to the requested materialized entity set in the organization.
     *
     * @param organizationId The id of the organization in which to synchronize the materialized entity set.
     * @param entitySetId The id of the entity set to synchronize.
     */
    @POST( BASE + ID_PATH + SET_ID_PATH + SYNCHRONIZE )
    Void synchronizeEdmChanges( @Path( ID ) UUID organizationId, @Path( SET_ID ) UUID entitySetId );

    /**
     * Refreshes the requested materialized entity set with data changes in the organization.
     *
     * @param organizationId The id of the organization in which to refresh the materialized entity sets data.
     * @param entitySetId The id of the entity set to refresh.
     */
    @POST( BASE + ID_PATH + SET_ID_PATH + REFRESH )
    Void refreshDataChanges( @Path( ID ) UUID organizationId, @Path( SET_ID ) UUID entitySetId );

    /**
     * Changes the refresh rate of a materialized entity set in the requested organization.
     *
     * @param organizationId The id of the organization in which to change the refresh rate of the materialized entity
     * set.
     * @param entitySetId The id of the entity set, whose refresh rate to change.
     * @param refreshRate The new refresh rate in minutes.
     */
    @PUT( BASE + ID_PATH + SET_ID_PATH + REFRESH_RATE )
    Void updateRefreshRate(
            @Path( ID ) UUID organizationId,
            @Path( SET_ID ) UUID entitySetId,
            @Body Integer refreshRate );

    /**
     * Disables automatic refresh of a materialized entity set in the requested organization.
     *
     * @param organizationId The id of the organization in which to disable automatic refresh of the materialized entity
     * set.
     * @param entitySetId The id of the entity set, which not to refresh automatically.
     */
    @DELETE( BASE + ID_PATH + SET_ID_PATH + REFRESH_RATE )
    Void deleteRefreshRate( @Path( ID ) UUID organizationId, @Path( SET_ID ) UUID entitySetId );

    @PUT( BASE + ID_PATH + TITLE )
    Void updateTitle( @Path( ID ) UUID organziationId, @Body String title );

    @PUT( BASE + ID_PATH + DESCRIPTION )
    Void updateDescription( @Path( ID ) UUID organizationId, @Body String description );

    @GET( BASE + ID_PATH + EMAIL_DOMAINS )
    Set<String> getAutoApprovedEmailDomains( @Path( ID ) UUID organizationId );

    @PUT( BASE + ID_PATH + EMAIL_DOMAINS )
    Void setAutoApprovedEmailDomain( @Path( ID ) UUID organizationId, @Body Set<String> emailDomain );

    /**
     * Add multiple e-mail domains to the auto-approval list.
     *
     * @param organizationId The id of the organization to modify.
     * @param emailDomains The e-mail domain to add to the auto-approval list.
     */
    @POST( BASE + ID_PATH + EMAIL_DOMAINS )
    Void addEmailDomains( @Path( ID ) UUID organizationId, @Body Set<String> emailDomains );

    @HTTP(
            method = "DELETE",
            hasBody = true,
            path = BASE + ID_PATH + EMAIL_DOMAINS )
    Void removeEmailDomains( @Path( ID ) UUID organizationId, @Body Set<String> emailDomain );

    /**
     * Adds a single e-mail domain to the auto-approval list. This will fail for users who are not an owner of the
     * organization.
     *
     * @param organizationId The id of the organization to modify.
     * @param emailDomain The e-mail domain to add to the auto-approval list.
     */
    @PUT( BASE + ID_PATH + EMAIL_DOMAINS + EMAIL_DOMAIN_PATH )
    Void addEmailDomain( @Path( ID ) UUID organizationId, @Path( EMAIL_DOMAIN ) String emailDomain );

    /**
     * Removes a single e-mail domain to the auto-approval list. This will fail for users who are not an owner of the
     * organization.
     *
     * @param organizationId The id of the organization to modify.
     * @param emailDomain The e-mail domain to add to the auto-approval list.
     */
    @DELETE( BASE + ID_PATH + EMAIL_DOMAINS + EMAIL_DOMAIN_PATH )
    Void removeEmailDomain( @Path( ID ) UUID organizationId, @Path( EMAIL_DOMAIN ) String emailDomain );

    //Endpoints about members
    @GET( BASE + ID_PATH + PRINCIPALS + MEMBERS )
    Iterable<OrganizationMember> getMembers( @Path( ID ) UUID organizationId );

    @PUT( BASE + ID_PATH + PRINCIPALS + MEMBERS + USER_ID_PATH )
    Void addMember( @Path( ID ) UUID organizationId, @Path( USER_ID ) String userId );

    @DELETE( BASE + ID_PATH + PRINCIPALS + MEMBERS + USER_ID_PATH )
    Void removeMember( @Path( ID ) UUID organizationId, @Path( USER_ID ) String userId );

    // Endpoints about roles
    @POST( BASE + ROLES )
    UUID createRole( @Body Role role );

    @GET( BASE + ID_PATH + PRINCIPALS + ROLES )
    Set<Role> getRoles( @Path( ID ) UUID organizationId );

    @GET( BASE + ID_PATH + PRINCIPALS + ROLES + ROLE_ID_PATH )
    Role getRole( @Path( ID ) UUID organizationId, @Path( ROLE_ID ) UUID roleId );

    @PUT( BASE + ID_PATH + PRINCIPALS + ROLES + ROLE_ID_PATH + TITLE )
    Void updateRoleTitle( @Path( ID ) UUID organizationId, @Path( ROLE_ID ) UUID roleId, @Body String title );

    @PUT( BASE + ID_PATH + PRINCIPALS + ROLES + ROLE_ID_PATH + DESCRIPTION )
    Void updateRoleDescription(
            @Path( ID ) UUID organizationId,
            @Path( ROLE_ID ) UUID roleId,
            @Body String description );

    @PUT( BASE + ID_PATH + PRINCIPALS + ROLES + ROLE_ID_PATH + GRANT )
    Void updateRoleGrant(
            @Path( ID ) UUID organizationId,
            @Path( ROLE_ID ) UUID roleId,
            @Nonnull @Body Grant grant );

    @DELETE( BASE + ID_PATH + PRINCIPALS + ROLES + ROLE_ID_PATH )
    Void deleteRole( @Path( ID ) UUID organizationId, @Path( ROLE_ID ) UUID roleId );

    @GET( BASE + ID_PATH + PRINCIPALS + ROLES + ROLE_ID_PATH + MEMBERS )
    Iterable<Auth0UserBasic> getAllUsersOfRole( @Path( ID ) UUID organizationId, @Path( ROLE_ID ) UUID roleId );

    @PUT( BASE + ID_PATH + PRINCIPALS + ROLES + ROLE_ID_PATH + MEMBERS + USER_ID_PATH )
    Void addRoleToUser( @Path( ID ) UUID organizationId, @Path( ROLE_ID ) UUID roleId, @Path( USER_ID ) String userId );

    @DELETE( BASE + ID_PATH + PRINCIPALS + ROLES + ROLE_ID_PATH + MEMBERS + USER_ID_PATH )
    Void removeRoleFromUser(
            @Path( ID ) UUID organizationId,
            @Path( ROLE_ID ) UUID roleId,
            @Path( USER_ID ) String userId );

    //Admin-only APIs
    @POST( BASE + ID_PATH + CONNECTIONS )
    Void addConnection( @Path( ID ) UUID organizationId, @Body Set<String> connections );

    @PUT( BASE + ID_PATH + CONNECTIONS )
    Void setConnections( @Path( ID ) UUID organizationId, @Body Set<String> connections );

    @HTTP( path = BASE + ID_PATH + CONNECTIONS, method = "DELETE", hasBody = true )
    Void removeConnections( @Path( ID ) UUID organizationId, @Body Set<String> connections );
}
