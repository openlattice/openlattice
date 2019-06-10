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
import retrofit2.http.*;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public interface OrganizationsApi {
    /*
     * These determine the service routing for the LB
     */
    // @formatter:off
    String SERVICE           = "/datastore";
    String CONTROLLER        = "/organizations";
    String BASE              = SERVICE + CONTROLLER;
    // @formatter:on
    /*
     * Acutal path elements
     */
    String ID                = "id";
    String ID_PATH           = "/{" + ID + "}";
    String DESCRIPTION       = "/description";
    String TITLE             = "/title";

    String ENTITY_SETS       = "/entity-sets";
    String INTEGRATION       = "/integration";
    String EMAIL_DOMAIN      = "email-domain";
    String EMAIL_DOMAINS     = "/email-domains";
    String EMAIL_DOMAIN_PATH = "/{" + EMAIL_DOMAIN + ":.+}";
    String PRINCIPALS        = "/principals";
    String PRINCIPAL_ID      = "pid";
    String PRINCIPAL_ID_PATH = "/{" + PRINCIPAL_ID + "}";
    String TYPE              = "type";
    String TYPE_PATH         = "/{" + TYPE + "}";
    String ROLES             = "/roles";
    String MEMBERS           = "/members";

    String ASSEMBLE          = "/assemble";
    String ROLE_ID           = "roleId";
    String ROLE_ID_PATH      = "/{" + ROLE_ID + "}";
    String USER_ID           = "userId";
    String USER_ID_PATH      = "/{" + USER_ID + ":.*}";

    String SYNCHRONIZE       = "/synchronize";
    String REFRESH           = "/refresh";
    String SET_ID            = "setId";
    String SET_ID_PATH       = "/{" + SET_ID + "}";

    String SET_PHONE         = "setPhone";
    String SET_PHONE_PATH    = "/{" + SET_PHONE + "}";


    @GET( BASE )
    Iterable<Organization> getOrganizations();

    @POST( BASE )
    UUID createOrganizationIfNotExists( @Body Organization organization );

    @GET( BASE + ID_PATH )
    Organization getOrganization( @Path( ID ) UUID organizationId );

    @DELETE( BASE + ID_PATH )
    Void destroyOrganization( @Path( ID ) UUID organizationId );

    @POST( BASE + ID_PATH + SET_PHONE_PATH )
    Void setOrganizationPhoneNumber( @Path( ID ) UUID organizationId, @Path( SET_PHONE ) String phoneNumber );

    @GET( BASE + ID_PATH + INTEGRATION )
    OrganizationIntegrationAccount getOrganizationIntegrationAccount(@Path(ID) UUID organizationId );

    /**
     * Retrieves all the organization entity sets without filtering.
     *
     * @param organizationId The id of the organization for which to retrieve entity sets
     *
     * @return All the organization entity sets along with flags indicating whether they are internal, external, and/or
     * materialized.
     */
    @GET( BASE + ID_PATH + ENTITY_SETS )
    Map<UUID,Set<OrganizationEntitySetFlag>> getOrganizationEntitySets( @Path( ID ) UUID organizationId);

    /**
     * Retrieves the entity sets belong to an organization matching a specified filter.
     *
     * @param organizationId The id of the organization for which to retrieve entity sets
     * @param flagFilter The set of flags for which to retrieve information.
     * @return All the organization entity sets along with flags indicating whether they are internal, external, and/or
     * materialized.
     */
    @POST( BASE + ID_PATH + ENTITY_SETS )
    Map<UUID,Set<OrganizationEntitySetFlag>> getOrganizationEntitySets(
            @Path( ID ) UUID organizationId,
            @Body EnumSet<OrganizationEntitySetFlag> flagFilter );

    /**
     * Materializes entity sets into the organization database.
     *
     * @param refreshRatesOfEntitySets The refresh rate of each entity set to assemble into materialized views mapped
     *                                 by their ids.
     */
    @POST( BASE + ID_PATH + ENTITY_SETS + ASSEMBLE )
    Map<UUID, Set<OrganizationEntitySetFlag>> assembleEntitySets(
            @Path( ID ) UUID organizationId,
            @Body Map<UUID, Integer> refreshRatesOfEntitySets );

    /**
     * Synchronizes EDM changes to the requested materialized entity set in the organization.
     *
     * @param organizationId The id of the organization in which to synchronize the materialized entity set.
     * @param entitySetId    The id of the entity set to synchronize.
     */
    @POST( BASE + ID_PATH + SET_ID_PATH + SYNCHRONIZE )
    Void synchronizeEdmChanges( @Path( ID ) UUID organizationId, @Path( SET_ID ) UUID entitySetId );

    /**
     * Refreshes the requested materialized entity set with data changes in the organization.
     *
     * @param organizationId The id of the organization in which to refresh the materialized entity sets data.
     * @param entitySetId    The id of the entity set to refresh.
     */
    @POST( BASE + ID_PATH + SET_ID_PATH + REFRESH )
    Void refreshDataChanges( @Path( ID ) UUID organizationId, @Path( SET_ID ) UUID entitySetId );

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
    Void addAutoApprovedEmailDomains( @Path( ID ) UUID organizationId, @Body Set<String> emailDomains );

    @HTTP(
            method = "DELETE",
            hasBody = true,
            path = BASE + ID_PATH + EMAIL_DOMAINS )
    Void removeAutoApprovedEmailDomains( @Path( ID ) UUID organizationId, @Body Set<String> emailDomain );

    /**
     * Adds a single e-mail domain to the auto-approval list. This will fail for users who are not an owner of the
     * organization.
     *
     * @param organizationId The id of the organization to modify.
     * @param emailDomain The e-mail domain to add to the auto-approval list.
     */
    @PUT( BASE + ID_PATH + EMAIL_DOMAINS + EMAIL_DOMAIN_PATH )
    Void addAutoApprovedEmailDomain( @Path( ID ) UUID organizationId, @Path( EMAIL_DOMAIN ) String emailDomain );

    /**
     * Removes a single e-mail domain to the auto-approval list. This will fail for users who are not an owner of the
     * organization.
     *
     * @param organizationId The id of the organization to modify.
     * @param emailDomain The e-mail domain to add to the auto-approval list.
     */
    @DELETE( BASE + ID_PATH + EMAIL_DOMAINS + EMAIL_DOMAIN_PATH )
    Void removeAutoApprovedEmailDomain( @Path( ID ) UUID organizationId, @Path( EMAIL_DOMAIN ) String emailDomain );

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

    @DELETE( BASE + ID_PATH + PRINCIPALS + ROLES + ROLE_ID_PATH )
    Void deleteRole( @Path( ID ) UUID organizationId, @Path( ROLE_ID ) UUID roleId );

    @GET( BASE + ID_PATH + PRINCIPALS + ROLES + ROLE_ID_PATH + MEMBERS )
    Iterable<Auth0UserBasic> getAllUsersOfRole( @Path( ID ) UUID organizationId, @Path( ROLE_ID ) UUID roleId );

    //    @GET( BASE + ID_PATH + MEMBERS )
    //    SetMultimap<SecurablePrincipal,SecurablePrincipal> getUsersAndRoles( UUID organizationsId );

    @PUT( BASE + ID_PATH + PRINCIPALS + ROLES + ROLE_ID_PATH + MEMBERS + USER_ID_PATH )
    Void addRoleToUser( @Path( ID ) UUID organizationId, @Path( ROLE_ID ) UUID roleId, @Path( USER_ID ) String userId );

    @DELETE( BASE + ID_PATH + PRINCIPALS + ROLES + ROLE_ID_PATH + MEMBERS + USER_ID_PATH )
    Void removeRoleFromUser(
            @Path( ID ) UUID organizationId,
            @Path( ROLE_ID ) UUID roleId,
            @Path( USER_ID ) String userId );

}
