package com.openlattice.organization

import com.auth0.json.mgmt.users.User
import com.openlattice.organization.roles.Role
import com.openlattice.organizations.*
import retrofit2.http.*
import java.util.*
import javax.annotation.Nonnull


/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
interface OrganizationsApi {
    companion object {
        // @formatter:off
        const val  SERVICE = "/datastore"
        const val  CONTROLLER = "/organizations"
        const val  BASE = SERVICE + CONTROLLER
        // @formatter:on

        const val ASSEMBLE = "/assemble"
        const val CONNECTIONS = "/connections"
        const val COUNT = "/count"
        const val DATABASE = "/database"
        const val DESCRIPTION = "/description"
        const val DESTROY = "/destroy"
        const val EMAIL_DOMAIN = "email-domain"
        const val EMAIL_DOMAINS = "/email-domains"
        const val EMAIL_DOMAIN_PATH = "/{$EMAIL_DOMAIN:.+}"
        const val ENTITY_SETS = "/entity-sets"
        const val GRANT = "/grant"
        const val ID = "id"
        const val ID_PATH = "/{$ID}"
        const val INTEGRATION = "/integration"
        const val MEMBERS = "/members"
        const val METADATA = "/metadata"
        const val METADATA_ENTITY_SET_IDS = "/metadata-entity-set-ids"
        const val PRINCIPALS = "/principals"
        const val PRINCIPAL_ID = "pid"
        const val PRINCIPAL_ID_PATH = "/{$PRINCIPAL_ID}"
        const val PROMOTE = "/promote"
        const val REFRESH = "/refresh"
        const val REFRESH_RATE = "/refresh-rate"
        const val ROLES = "/roles"
        const val ROLE_ID = "roleId"
        const val ROLE_ID_PATH = "/{$ROLE_ID}"

        const val SET_ID = "setId"
        const val SET_ID_PATH = "/{$SET_ID}"
        const val SYNCHRONIZE = "/synchronize"
        const val TITLE = "/title"
        const val TRANSPORT = "/transport"
        const val TYPE = "type"
        const val TYPE_PATH = "/{$TYPE}"
        const val USER_ID = "userId"
        const val USER_ID_PATH = "/{$USER_ID:.*}"

        const val DATASOURCES = "/datasources"
        const val DATASOURCE_ID = "datasource_id"
        const val DATASOURCE_ID_PATH = "/{$DATASOURCE_ID}"

    }

    // @formatter:on
    @GET(BASE)
    fun getOrganizations(): Iterable<Organization>

    @GET(BASE + METADATA)
    fun getMetadataOfOrganizations(): Iterable<OrganizationPrincipal>

    @POST(BASE)
    fun createOrganizationIfNotExists(@Body organization: Organization): UUID

    @GET(BASE + ID_PATH)
    fun getOrganization(@Path(ID) organizationId: UUID): Organization?

    @DELETE(BASE + ID_PATH)
    fun destroyOrganization(@Path(ID) organizationId: UUID): Void?

    @GET(BASE + ID_PATH + INTEGRATION)
    fun getOrganizationIntegrationAccount(
            @Path(ID) organizationId: UUID
    ): OrganizationIntegrationAccount

    /**
     * Marks an entity set for transporter
     */
    @GET(BASE + ID_PATH + SET_ID_PATH + TRANSPORT)
    fun transportEntitySet(
            @Path(ID) organizationId: UUID, @Path(
                    SET_ID
            ) entitySetId: UUID
    ): Void?

    /**
     * Destroys an transported entity set
     */
    @GET(BASE + ID_PATH + SET_ID_PATH + DESTROY)
    fun destroyTransportedEntitySet(
            @Path(ID) organizationId: UUID, @Path(
                    SET_ID
            ) entitySetId: UUID
    ): Void?

    /**
     * Rolls the organization integration account credentials.
     *
     * @param organizationId The organization account for which to roll credentials.
     * @return The new organization account credentials.
     */
    @PATCH(BASE + ID_PATH + INTEGRATION)
    fun rollOrganizationIntegrationAccount(
            @Path(ID) organizationId: UUID
    ): OrganizationIntegrationAccount

    /**
     * Retrieves all the organization entity sets without filtering.
     *
     * @param organizationId The id of the organization for which to retrieve entity sets
     * @return All the organization entity sets along with flags indicating whether they are internal, external, and/or
     * materialized.
     */
    @GET(BASE + ID_PATH + ENTITY_SETS)
    fun getOrganizationEntitySets(
            @Path(ID) organizationId: UUID
    ): Map<UUID, Set<OrganizationEntitySetFlag>>

    /**
     * Retrieves the entity sets belong to an organization matching a specified filter.
     *
     * @param organizationId The id of the organization for which to retrieve entity sets
     * @param flagFilter     The set of flags for which to retrieve information.
     * @return All the organization entity sets along with flags indicating whether they are internal, external, and/or
     * materialized.
     */
    @POST(BASE + ID_PATH + ENTITY_SETS)
    fun getOrganizationEntitySets(
            @Path(ID) organizationId: UUID,
            @Body flagFilter: EnumSet<OrganizationEntitySetFlag>
    ): Map<UUID, Set<OrganizationEntitySetFlag>>

    /**
     * Materializes entity sets into the organization database.
     *
     * @param refreshRatesOfEntitySets The refresh rate in minutes of each entity set to assemble into materialized
     * views mapped by their ids.
     */
    @POST(BASE + ID_PATH + ENTITY_SETS + ASSEMBLE)
    fun assembleEntitySets(
            @Path(ID) organizationId: UUID,
            @Body refreshRatesOfEntitySets: Map<UUID, Int>
    ): Map<UUID, Set<OrganizationEntitySetFlag>>

    /**
     * Synchronizes EDM changes to the requested materialized entity set in the organization.
     *
     * @param organizationId The id of the organization in which to synchronize the materialized entity set.
     * @param entitySetId    The id of the entity set to synchronize.
     */
    @POST(BASE + ID_PATH + SET_ID_PATH + SYNCHRONIZE)
    fun synchronizeEdmChanges(
            @Path(ID) organizationId: UUID, @Path(
                    SET_ID
            ) entitySetId: UUID
    ): Void?

    /**
     * Refreshes the requested materialized entity set with data changes in the organization.
     *
     * @param organizationId The id of the organization in which to refresh the materialized entity sets data.
     * @param entitySetId    The id of the entity set to refresh.
     */
    @POST(BASE + ID_PATH + SET_ID_PATH + REFRESH)
    fun refreshDataChanges(
            @Path(ID) organizationId: UUID, @Path(
                    SET_ID
            ) entitySetId: UUID
    ): Void?

    /**
     * Changes the refresh rate of a materialized entity set in the requested organization.
     *
     * @param organizationId The id of the organization in which to change the refresh rate of the materialized entity
     * set.
     * @param entitySetId    The id of the entity set, whose refresh rate to change.
     * @param refreshRate    The new refresh rate in minutes.
     */
    @PUT(BASE + ID_PATH + SET_ID_PATH + REFRESH_RATE)
    fun updateRefreshRate(
            @Path(ID) organizationId: UUID,
            @Path(SET_ID) entitySetId: UUID,
            @Body refreshRate: Int
    ): Void?

    /**
     * Disables automatic refresh of a materialized entity set in the requested organization.
     *
     * @param organizationId The id of the organization in which to disable automatic refresh of the materialized entity
     * set.
     * @param entitySetId    The id of the entity set, which not to refresh automatically.
     */
    @DELETE(
            BASE + ID_PATH + SET_ID_PATH + REFRESH_RATE
    )
    fun deleteRefreshRate(
            @Path(ID) organizationId: UUID, @Path(
                    SET_ID
            ) entitySetId: UUID
    ): Void?

    @PUT(BASE + ID_PATH + TITLE)
    fun updateTitle(@Path(ID) organziationId: UUID, @Body title: String): Void?

    @PUT(BASE + ID_PATH + DESCRIPTION)
    fun updateDescription(@Path(ID) organizationId: UUID, @Body description: String): Void?

    @GET(BASE + ID_PATH + EMAIL_DOMAINS)
    fun getAutoApprovedEmailDomains(@Path(ID) organizationId: UUID): Set<String>

    @PUT(BASE + ID_PATH + EMAIL_DOMAINS)
    fun setAutoApprovedEmailDomain(
            @Path(ID) organizationId: UUID, @Body emailDomain: Set<String>
    ): Void?

    /**
     * Add multiple e-mail domains to the auto-approval list.
     *
     * @param organizationId The id of the organization to modify.
     * @param emailDomains   The e-mail domain to add to the auto-approval list.
     */
    @POST(BASE + ID_PATH + EMAIL_DOMAINS)
    fun addEmailDomains(@Path(ID) organizationId: UUID, @Body emailDomains: Set<String>): Void?

    @HTTP(
            method = "DELETE", hasBody = true,
            path = BASE + ID_PATH + EMAIL_DOMAINS
    )
    fun removeEmailDomains(@Path(ID) organizationId: UUID, @Body emailDomain: Set<String>): Void?

    /**
     * Adds a single e-mail domain to the auto-approval list. This will fail for users who are not an owner of the
     * organization.
     *
     * @param organizationId The id of the organization to modify.
     * @param emailDomain    The e-mail domain to add to the auto-approval list.
     */
    @PUT(
            BASE + ID_PATH + EMAIL_DOMAINS + EMAIL_DOMAIN_PATH
    )
    fun addEmailDomain(
            @Path(ID) organizationId: UUID, @Path(
                    EMAIL_DOMAIN
            ) emailDomain: String
    ): Void?

    /**
     * Removes a single e-mail domain to the auto-approval list. This will fail for users who are not an owner of the
     * organization.
     *
     * @param organizationId The id of the organization to modify.
     * @param emailDomain    The e-mail domain to add to the auto-approval list.
     */
    @DELETE(
            BASE + ID_PATH + EMAIL_DOMAINS + EMAIL_DOMAIN_PATH
    )
    fun removeEmailDomain(
            @Path(ID) organizationId: UUID, @Path(
                    EMAIL_DOMAIN
            ) emailDomain: String
    ): Void?

    //Endpoints about members
    @GET(BASE + ID_PATH + PRINCIPALS + MEMBERS)
    fun getMembers(@Path(ID) organizationId: UUID): Iterable<OrganizationMember>

    @GET(
            BASE + PRINCIPALS + MEMBERS + COUNT
    )
    fun getMemberCountForOrganizations(@Body organizationIds: Set<UUID>): Map<UUID, Int>

    @GET(BASE + PRINCIPALS + ROLES + COUNT)
    fun getRoleCountForOrganizations(@Body organizationIds: Set<UUID>): Map<UUID, Int>

    @PUT(
            BASE + ID_PATH + PRINCIPALS + MEMBERS + USER_ID_PATH
    )
    fun addMember(
            @Path(ID) organizationId: UUID, @Path(
                    USER_ID
            ) userId: String
    ): Void?

    @DELETE(
            BASE + ID_PATH + PRINCIPALS + MEMBERS + USER_ID_PATH
    )
    fun removeMember(
            @Path(ID) organizationId: UUID, @Path(
                    USER_ID
            ) userId: String
    ): Void?

    // Endpoints about roles
    @POST(BASE + ROLES)
    fun createRole(@Body role: Role): UUID

    @GET(
            BASE + ID_PATH + PRINCIPALS + ROLES
    )
    fun getRoles(@Path(ID) organizationId: UUID): Set<Role>

    @GET(
            BASE + ID_PATH + PRINCIPALS + ROLES + ROLE_ID_PATH
    )
    fun getRole(@Path(ID) organizationId: UUID, @Path(ROLE_ID) roleId: UUID): Role

    @PUT(
            BASE + ID_PATH + PRINCIPALS + ROLES + ROLE_ID_PATH + TITLE
    )
    fun updateRoleTitle(
            @Path(ID) organizationId: UUID, @Path(
                    ROLE_ID
            ) roleId: UUID, @Body title: String
    ): Void?

    @PUT(
            BASE + ID_PATH + PRINCIPALS + ROLES + ROLE_ID_PATH + DESCRIPTION
    )
    fun updateRoleDescription(
            @Path(ID) organizationId: UUID,
            @Path(ROLE_ID) roleId: UUID,
            @Body description: String
    ): Void?

    /**
     * Each organization can have one grant for each grant type per role. Most grant types accept multiple mappings
     * if multiple settings are required.
     *
     * @param organizationId The organziation id for which to update the grant.
     * @param roleId         The role for which to update the grant.
     * @param grant          The grant to update.
     * @return Nothing.
     */
    @PUT(
            BASE + ID_PATH + PRINCIPALS + ROLES + ROLE_ID_PATH + GRANT
    )
    fun updateRoleGrant(
            @Path(ID) organizationId: UUID,
            @Path(ROLE_ID) roleId: UUID,
            @Nonnull @Body grant: Grant
    ): Void?

    @DELETE(
            BASE + ID_PATH + PRINCIPALS + ROLES + ROLE_ID_PATH
    )
    fun deleteRole(
            @Path(ID) organizationId: UUID, @Path(
                    ROLE_ID
            ) roleId: UUID
    ): Void?

    @GET(
            BASE + ID_PATH + PRINCIPALS + ROLES + ROLE_ID_PATH + MEMBERS
    )
    fun getAllUsersOfRole(
            @Path(ID) organizationId: UUID, @Path(
                    ROLE_ID
            ) roleId: UUID
    ): Iterable<User>

    @PUT(
            BASE + ID_PATH + PRINCIPALS + ROLES + ROLE_ID_PATH + MEMBERS + USER_ID_PATH
    )
    fun addRoleToUser(
            @Path(ID) organizationId: UUID, @Path(
                    ROLE_ID
            ) roleId: UUID, @Path(USER_ID) userId: String
    ): Void?

    @DELETE(
            BASE + ID_PATH + PRINCIPALS + ROLES + ROLE_ID_PATH + MEMBERS + USER_ID_PATH
    )
    fun removeRoleFromUser(
            @Path(ID) organizationId: UUID,
            @Path(ROLE_ID) roleId: UUID,
            @Path(USER_ID) userId: String
    ): Void?

    //Admin-only APIs
    @POST(BASE + ID_PATH + CONNECTIONS)
    fun addConnections(@Path(ID) organizationId: UUID, @Body connections: Set<String>): Void?

    @PUT(BASE + ID_PATH + CONNECTIONS)
    fun setConnections(@Path(ID) organizationId: UUID, @Body connections: Set<String>): Void?

    @PUT(BASE + ID_PATH + METADATA_ENTITY_SET_IDS)
    fun setMetadataEntitySetIds(
            @Path(ID) organizationId: UUID,
            @Body entitySetIds: OrganizationMetadataEntitySetIds
    ): Void?

    @POST(BASE + ID_PATH + METADATA)
    fun importMetadata(@Path(ID) organizationId: UUID): Void?

    @HTTP(
            path = BASE + ID_PATH + CONNECTIONS, method = "DELETE",
            hasBody = true
    )
    fun removeConnections(@Path(ID) organizationId: UUID, @Body connections: Set<String>): Void?

    /**
     * Moves the specified table from the staging schema to the openlattice schema in the organization's external database
     */
    @POST(BASE + PROMOTE + ID_PATH)
    fun promoteStagingTable(@Path(ID) organizationId: UUID, @Body tableName: String): Void?

    @GET(BASE + ID_PATH + DATABASE)
    fun getOrganizationDatabaseName(@Path(ID) organizationId: UUID): String

    @PATCH(BASE + ID_PATH + DATABASE)
    fun renameOrganizationDatabase(
            @Path(ID) organizationId: UUID, @Body newDatabaseName: String
    ): Void?

    /**
     * Lists the datasources registered in an organization.
     *
     * @param organizationId The id of the organization
     *
     */
    @GET(BASE + ID_PATH + DATASOURCE)
    fun listDataSources(@Path(ID) organizationId: UUID): Map<UUID, JdbcConnection>

    /**
     * Registers a datasource with an organization.
     *
     * @return The id assigned to the newly registered datasource.
     */
    @POST(BASE + ID_PATH + DATASOURCE)
    fun registerDataSource(
            @Path(ID) organizationId: UUID,
            @Body dataSource: JdbcConnection
    ): UUID

    /**
     * Updates a datasource in an organization.
     *
     * @param organizationId The id of the organization
     * @param datasourceId The id of the data source to update
     * @param datasource The datasource information to set.
     *
     */
    @PUT(BASE + ID_PATH + DATASOURCE + DATASOURCE_ID_PATH)
    fun updateDataSource(
            @Path(ID) organizationId: UUID,
            @Path(DATASOURCE) dataSourceId: UUID,
            @Body dataSource: JdbcConnection
    )
}
