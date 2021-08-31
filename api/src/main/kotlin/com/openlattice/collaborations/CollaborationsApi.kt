package com.openlattice.collaborations

import com.openlattice.organizations.Organization
import com.openlattice.organizations.OrganizationDatabase
import com.openlattice.authorization.Permission
import retrofit2.http.*
import java.util.*

interface CollaborationsApi {

    companion object {

        const val SERVICE = "/datastore"
        const val CONTROLLER = "/collaborations"
        const val BASE = SERVICE + CONTROLLER

        const val ALL_PATH = "/all"
        const val DATABASE_PATH = "/database"
        const val DATA_SETS_PATH = "/datasets"
        const val ORGANIZATIONS_PATH = "/organizations"
        const val PROJECT_PATH = "/project"

        const val COLLABORATION_ID_PARAM = "collaborationId"
        const val COLLABORATION_ID_PATH = "/{$COLLABORATION_ID_PARAM}"
        const val DATA_SET_ID_PARAM = "dataSetId"
        const val DATA_SET_ID_PATH = "/{$DATA_SET_ID_PARAM}"
        const val ID_PARAM = "id"
        const val ORGANIZATION_ID_PARAM = "organizationId"
        const val ORGANIZATION_ID_PATH = "/{$ORGANIZATION_ID_PARAM}"
    }

    /**
     * Gets all [Collaboration] objects the caller has [Permission.READ] on.
     *
     * @return a list of [Collaboration] objects
     */
    @GET(BASE + ALL_PATH)
    fun getCollaborations(): Iterable<Collaboration>

    /**
     * Gets [Collaboration] objects the caller has [Permission.READ] on with the provided ids.
     *
     * @return a list of [Collaboration] objects
     */
    @GET(BASE)
    fun getCollaborationsWithId(@Query(ID_PARAM) ids: Set<UUID>): Map<UUID, Collaboration>

    /**
     * Gets the [Collaboration] object with the given collaboration id. The caller must have [Permission.READ] on the
     * target [Collaboration] object.
     *
     * @param collaborationId [Collaboration] id
     * @return the target [Collaboration] object
     */
    @GET(BASE + COLLABORATION_ID_PATH)
    fun getCollaboration(@Path(COLLABORATION_ID_PARAM) collaborationId: UUID): Collaboration

    /**
     * Gets all [Collaboration] objects the caller has [Permission.READ] on that include the given organization id. The
     * caller must have [Permission.READ] on the target [Organization] object.
     *
     * @param organizationId [Organization] id
     * @return a list of [Collaboration] objects
     */
    @GET(BASE + ORGANIZATIONS_PATH + ORGANIZATION_ID_PATH)
    fun getCollaborationsWithOrganization(@Path(ORGANIZATION_ID_PARAM) organizationId: UUID): Iterable<Collaboration>

    /**
     * Creates a new [Collaboration] object.
     *
     * @param collaboration the [Collaboration] object to create
     * @return the id of the newly created [Collaboration] object
     */
    @POST(BASE)
    fun createCollaboration(@Body collaboration: Collaboration): UUID

    /**
     * Deletes the [Collaboration] object with the given collaboration id. The caller must have [Permission.OWNER] on
     * the target [Collaboration] object.
     *
     * @param collaborationId [Collaboration] id
     */
    @DELETE(BASE + COLLABORATION_ID_PATH)
    fun deleteCollaboration(@Path(COLLABORATION_ID_PARAM) collaborationId: UUID)

    /**
     * Adds the given organization ids to the [Collaboration] object with the given collaboration id. The caller must
     * have [Permission.OWNER] on the target [Collaboration] object.
     *
     * @param collaborationId [Collaboration] id
     * @param organizationIds a set of [Organization] ids
     */
    @PATCH(BASE + COLLABORATION_ID_PATH + ORGANIZATIONS_PATH)
    fun addOrganizationsToCollaboration(
        @Path(COLLABORATION_ID_PARAM) collaborationId: UUID,
        @Body organizationIds: Set<UUID>
    )

    /**
     * Removes the given organization ids from the [Collaboration] object with the given collaboration id. The caller
     * must have [Permission.OWNER] on the target [Collaboration] object.
     *
     * @param collaborationId [Collaboration] id
     * @param organizationIds a set of [Organization] ids
     */
    @DELETE(BASE + COLLABORATION_ID_PATH + ORGANIZATIONS_PATH)
    fun removeOrganizationsFromCollaboration(
        @Path(COLLABORATION_ID_PARAM) collaborationId: UUID,
        @Body organizationIds: Set<UUID>
    )

    /**
     * Gets the database info for the [Collaboration] object with the given collaboration id. The caller must have
     * [Permission.READ] on the target [Collaboration] object.
     *
     * @param collaborationId [Collaboration] id
     * @return the database info for the target [Collaboration] object
     */
    @GET(BASE + COLLABORATION_ID_PATH + DATABASE_PATH)
    fun getCollaborationDatabaseInfo(@Path(COLLABORATION_ID_PARAM) collaborationId: UUID): OrganizationDatabase

    /**
     * Renames the database belonging to the [Collaboration] object with the given collaboration id. The caller must
     * have [Permission.OWNER] on the target [Collaboration] object.
     *
     * @param collaborationId a [Collaboration] id
     * @param name the new database name
     */
    @PATCH(BASE + COLLABORATION_ID_PATH + DATABASE_PATH)
    fun renameCollaborationDatabase(@Path(COLLABORATION_ID_PARAM) collaborationId: UUID, @Body name: String)

    /**
     * Adds the given data set id to the [Collaboration] object with the given collaboration id. The target data set
     * object must belong to the [Organization] object with the given organization id. The caller must have:
     *   - [Permission.READ] on the target [Collaboration] object
     *   - [Permission.READ] on the target [Organization] object
     *   - [Permission.OWNER] on the target data set object.
     *
     * @param collaborationId [Collaboration] id
     * @param organizationId [Organization] id
     * @param dataSetId data set id
     */
    @PATCH(BASE + COLLABORATION_ID_PATH + PROJECT_PATH + ORGANIZATION_ID_PATH + DATA_SET_ID_PATH)
    fun addDataSetToCollaboration(
        @Path(COLLABORATION_ID_PARAM) collaborationId: UUID,
        @Path(ORGANIZATION_ID_PARAM) organizationId: UUID,
        @Path(DATA_SET_ID_PARAM) dataSetId: UUID
    )

    /**
     * Removes the given data set id from the [Collaboration] object with the given collaboration id. The target data
     * set object must belong to the [Organization] object with the given organization id. The caller must have:
     *   - [Permission.READ] on the target [Collaboration] object
     *   - [Permission.READ] on the target [Organization] object
     *   - [Permission.OWNER] on the target data set object.
     *
     * @param collaborationId [Collaboration] id
     * @param organizationId [Organization] id
     * @param dataSetId data set id
     */
    @DELETE(BASE + COLLABORATION_ID_PATH + PROJECT_PATH + ORGANIZATION_ID_PATH + DATA_SET_ID_PATH)
    fun removeDataSetFromCollaboration(
        @Path(COLLABORATION_ID_PARAM) collaborationId: UUID,
        @Path(ORGANIZATION_ID_PARAM) organizationId: UUID,
        @Path(DATA_SET_ID_PARAM) dataSetId: UUID
    )

    /**
     * Gets all data set ids that belong to the [Organization] object with the given organization id that are added
     * to any [Collaboration] objects that the caller has [Permission.READ] on. The caller must have [Permission.READ]
     * on the target [Organization] object.
     *
     * @param organizationId [Organization] id
     * @return Map<K, V> where K is [Collaboration] id and V is a list of data set ids
     */
    @GET(BASE + ORGANIZATIONS_PATH + ORGANIZATION_ID_PATH + DATA_SETS_PATH)
    fun getOrganizationCollaborationDataSets(@Path(ORGANIZATION_ID_PARAM) organizationId: UUID): Map<UUID, List<UUID>>

    /**
     * Gets all data set ids that are added to the [Collaboration] object with the given collaboration id that belong
     * to any [Organization] objects that the caller has [Permission.READ] on. The caller must have [Permission.READ]
     * on the target [Collaboration] object.
     *
     * @param collaborationId [Collaboration] id
     * @return Map<K, V> where K is [Organization] id and V is a list of data set ids
     */
    @GET(BASE + COLLABORATION_ID_PATH + DATA_SETS_PATH)
    fun getCollaborationDataSets(@Path(COLLABORATION_ID_PARAM) collaborationId: UUID): Map<UUID, List<UUID>>

    /**
     * Gets all [Collaboration] ids that the caller has [Permission.READ] on that include the given data set ids that
     * the caller has [Permission.READ] on.
     *
     * @param dataSetIds a set of data set ids
     * @return Map<K, V> where K is a data set id and V is a list of [Collaboration] ids
     */
    @POST(BASE + DATA_SETS_PATH)
    fun getCollaborationsWithDataSets(@Body dataSetIds: Set<UUID>): Map<UUID, List<UUID>>
}
