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
import com.openlattice.edm.requests.MetadataUpdate;
import com.openlattice.organization.roles.Role;

import java.util.*;

import retrofit2.http.*;

public interface OrganizationsApi {
    String ASSEMBLE                 = "/assemble";
    String CONTROLLER               = "/organizations";
    String DESCRIPTION              = "/description";
    String EMAIL_DOMAIN             = "email-domain";
    String EMAIL_DOMAINS            = "/email-domains";
    String EMAIL_DOMAIN_PATH        = "/{" + EMAIL_DOMAIN + ":.+}";
    String ENTITY_SETS              = "/entity-sets";
    String EXTERNAL_DATABASE        = "/external-database";
    String EXTERNAL_DATABASE_COLUMN = "/external-database-column";
    String EXTERNAL_DATABASE_TABLE  = "/external-database-table";
    String PRIMARY_KEY              = "/primary-key";
    // @formatter:on
    /*
     * Acutal path elements
     */
    String ID                       = "id";
    String ID_PATH                  = "/{" + ID + "}";
    String INTEGRATION              = "/integration";
    String MEMBERS                  = "/members";
    String PRINCIPALS               = "/principals";
    String PRINCIPAL_ID             = "pid";
    String PRINCIPAL_ID_PATH        = "/{" + PRINCIPAL_ID + "}";
    String REFRESH                  = "/refresh";
    String REFRESH_RATE             = "/refresh-rate";
    String ROLES                    = "/roles";
    String ROLE_ID                  = "roleId";
    String ROLE_ID_PATH             = "/{" + ROLE_ID + "}";
    String TABLE_NAME               = "tableName";
    String TABLE_NAME_PATH          = "/{" + TABLE_NAME + "}";
    String COLUMN_NAME              = "columnName";
    String COLUMN_NAME_PATH         = "/{" + COLUMN_NAME + "}";
    String SQL_TYPE                 = "sqlType";
    String SQL_TYPE_PATH            = "/{" + SQL_TYPE + "}";

    /*
     * These determine the service routing for the LB
     */
    // @formatter:off
    String SERVICE           = "/datastore";
    String BASE              = SERVICE + CONTROLLER;
    String SET_ID       = "setId";
    String SET_ID_PATH  = "/{" + SET_ID + "}";
    String SYNCHRONIZE  = "/synchronize";
    String TITLE             = "/title";
    String TYPE              = "type";
    String TYPE_PATH         = "/{" + TYPE + "}";
    String USER_ID      = "userId";
    String USER_ID_PATH = "/{" + USER_ID + ":.*}";

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
     * @param organizationId The id of the organization in which to disable automatic refresh of the materialized entity
     *                       set.
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

    //Endpoints for management of atlas database

    //create
    /**
     * Creates a securable OrganizationExternalDatabaseTable object, which represents an
     * organization's table in an external database. The table must already exist in the database
     * @param organizationId The organization's UUID
     * @param organizationExternalDatabaseTable The object to be created
     */
    @POST( BASE + ID_PATH + EXTERNAL_DATABASE_TABLE )
    UUID createExternalDatabaseTable( @Path(ID) UUID organizationId,
            @Body OrganizationExternalDatabaseTable organizationExternalDatabaseTable );

    /**
     * Creates a securable OrganizationExternalDatabaseColumn object, which represents a
     * column within an organization's table in an external database. The table and column must
     * already exist in the database
     * @param organizationId The organization's UUID
     * @param organizationExternalDatabaseColumn The object to be created
     */
    @POST( BASE + ID_PATH + EXTERNAL_DATABASE_COLUMN )
    UUID createExternalDatabaseColumn( @Path( ID ) UUID organizationId,
            @Body OrganizationExternalDatabaseColumn organizationExternalDatabaseColumn );

    /**
     * Creates a table with specified columns within an organization's database and creates a
     * securable OrganizationExternalDatabaseTable object and the specified number of
     * securable OrganizationExternalDatabaseColumn objects, which correspond to the new table.
     * The table must not already exist in the database
     * @param organizationId The organization's UUID
     * @param tableName The name of the table to be created
     * @param columnNameToSqlType An ordered map (of column name to sql type) of columns to be created
     */
    @POST( BASE + ID_PATH + TABLE_NAME_PATH + EXTERNAL_DATABASE_TABLE )
    Void createNewExternalDatabaseTable( @Path( ID ) UUID organizationId, @Path( TABLE_NAME ) String tableName,
            @Body LinkedHashMap<String, String> columnNameToSqlType);

    /**
     * Creates a new column within a table belonging to an organization and creates a
     * securable OrganizationExternalDatabaseColumn object corresponding to that column.
     * The column must not already exist in the database
     * @param organizationId The organization's UUID
     * @param tableName The name of the table to which the column will belong
     * @param columnName The name of the column to be created
     * @param sqlType The sql type of the column to be created
     */
    @POST( BASE + ID_PATH + TABLE_NAME_PATH + COLUMN_NAME_PATH + SQL_TYPE_PATH + EXTERNAL_DATABASE_COLUMN )
    Void createNewExternalDatabaseColumn(
            @Path( ID ) UUID organizationId,
            @Path( TABLE_NAME ) String tableName,
            @Path( COLUMN_NAME ) String columnName,
            @Path( SQL_TYPE) String sqlType);

    /**
     *
     * @param organizationId The organization's UUID
     * @param userId The user to be given trusted status
     * @param ipAddresses The set of ip addresses (as strings) from which users will be trusted
     */
    @POST( BASE + ID_PATH + USER_ID_PATH + EXTERNAL_DATABASE)
    Void addTrustedUser(
            @Path( ID ) UUID organizationId,
            @Path( USER_ID ) String userId,
            @Body Set<String> ipAddresses );

    //get
    /**
     * Gets an OrganizationExternalDatabaseTable object, which represents an
     * organization's table in an external database
     * @param organizationId The organization's UUID
     * @param tableName The exact name of the table in the database
     */
    @GET( BASE + ID_PATH + TABLE_NAME_PATH + EXTERNAL_DATABASE_TABLE)
    OrganizationExternalDatabaseTable getExternalDatabaseTable(
            @Path( ID ) UUID organizationId, @Path( TABLE_NAME) String tableName );

    /**
     * Gets an OrganizationExternalDatabaseColumn object, which represents a column
     * within an organization's table in an external database
     * @param organizationId The organization's UUID
     * @param tableName The exact name of the table in the database
     * @param columnName The exact name of the column in the database
     */
    @GET( BASE + ID_PATH + TABLE_NAME_PATH + COLUMN_NAME_PATH + EXTERNAL_DATABASE_COLUMN)
    OrganizationExternalDatabaseColumn getExternalDatabaseColumn(
            @Path( ID ) UUID organizationId, @Path(TABLE_NAME) String tableName, @Path( COLUMN_NAME) String columnName );


    //update
    /**
     * Updates an OrganizationExternalDatabaseTable object, which represents an
     * organization's table in an external database. This alters both the object
     * and the table in the database.
     * @param organizationId The organization's UUID
     * @param tableName The exact name of the table in the database
     */
    @PATCH( BASE + ID_PATH + TABLE_NAME_PATH + EXTERNAL_DATABASE_TABLE)
    Void updateExternalDatabaseTable(
            @Path( ID ) UUID organizationId,
            @Path( TABLE_NAME ) String tableName,
            @Body MetadataUpdate metadataUpdate );

    /**
     * Updates an OrganizationExternalDatabaseColumn object, which represents a column
     * within an organization's table in an external database. This alters both the object
     * and the column in the database.
     * @param organizationId The organization's UUID
     * @param tableName The exact name of the table in the database
     * @param columnName The exact name of the column in the database
     */
    @PATCH( BASE + ID_PATH + TABLE_NAME_PATH + COLUMN_NAME_PATH + EXTERNAL_DATABASE_COLUMN)
    Void updateExternalDatabaseColumn(
            @Path( ID ) UUID organizationId,
            @Path( TABLE_NAME ) String tableName,
            @Path( COLUMN_NAME ) String columnName,
            @Body MetadataUpdate metadataUpdate );

    /**
     * Sets the primary key on a table.
     * @param organizationId The organization's UUID
     * @param tableName The exact name of the table in the database
     * @param columnNames The exact names of the column(s) that will comprise the primary key.
     *                    An empty set will remove the primary key from the table.
     */
    @PATCH( BASE + ID_PATH + TABLE_NAME_PATH + PRIMARY_KEY )
    Void setPrimaryKey(
            @Path( ID ) UUID organizationId,
            @Path ( TABLE_NAME ) String tableName,
            @Body Set<String> columnNames );


    //delete
    /**
     * Deletes an OrganizationExternalDatabaseTable object, which represents an
     * organization's table in an external database. This deletes both the object
     * and the table in the database. It is a hard delete
     * @param organizationId The organization's UUID
     * @param tableName The exact name of the table in the database
     */
    @DELETE(BASE + ID_PATH + TABLE_NAME_PATH + EXTERNAL_DATABASE_TABLE)
    Void deleteExternalDatabaseTable(
            @Path( ID ) UUID organizationId,
            @Path( TABLE_NAME ) String tableName );

    /**
     * Deletes an OrganizationExternalDatabaseColumn object, which represents a column
     * within an organization's table in an external database. This deletes both the object
     * and the column in the database. It is a hard delete
     * @param organizationId The organization's UUID
     * @param tableName The exact name of the table in the database
     * @param columnName The exact name of the column in the database
     */
    @DELETE(BASE + ID_PATH + TABLE_NAME_PATH + COLUMN_NAME_PATH + EXTERNAL_DATABASE_COLUMN)
    Void deleteExternalDatabaseColumn(
            @Path( ID ) UUID organizationId,
            @Path( TABLE_NAME ) String tableName,
            @Path( COLUMN_NAME ) String columnName);

    /**
     * Deletes multiple OrganizationExternalDatabaseTable objects and the tables they represent
     * in the database. It is a hard delete
     * @param organizationId The organization's UUID
     * @param tableNames The exact names of the tables in the database
     */
    @DELETE(BASE + ID_PATH + EXTERNAL_DATABASE_TABLE)
    Void deleteExternalDatabaseTables(
            @Path( ID ) UUID organizationId,
            @Body Set<String> tableNames );
    /**
     * Deletes multiple OrganizationExternalDatabaseColumn objects and the columns they represent
     * within an organization's table in an external database. It is a hard delete
     * @param organizationId The organization's UUID
     * @param tableName The exact name of the table in the database
     * @param columnNames The exact names of the columns in the database
     */
    @DELETE( BASE + ID_PATH + TABLE_NAME_PATH + EXTERNAL_DATABASE_COLUMN)
    Void deleteExternalDatabaseColumns(
            @Path( ID ) UUID organizationId,
            @Path( TABLE_NAME ) String tableName,
            @Body Set<String> columnNames );

}