package com.openlattice.collaborations

import com.openlattice.organizations.OrganizationDatabase
import retrofit2.http.*
import java.util.*

interface CollaborationsApi {

    companion object {
        // @formatter:off
        const val  SERVICE = "/datastore"
        const val  CONTROLLER = "/collaborations"
        const val  BASE = SERVICE + CONTROLLER
        // @formatter:on

        const val DATABASE_PATH = "/database"
        const val ORGANIZATIONS_PATH = "/organizations"
        const val PROJECT_PATH = "/project"

        const val ID = "id"
        const val ID_PATH = "/{$ID}"
        const val ORGANIZATION_ID = "organizationId"
        const val ORGANIZATION_ID_PATH = "/{$ORGANIZATION_ID}"
        const val TABLE_ID = "tableId"
        const val TABLE_ID_PATH = "/{$TABLE_ID}"
    }

    @GET(BASE)
    fun getCollaborations(): Iterable<Collaboration>

    @POST(BASE)
    fun createCollaboration(@Body collaboration: Collaboration): UUID

    @GET(BASE + ID_PATH)
    fun getCollaboration(@Path(ID) id: UUID): Collaboration

    @GET(BASE + ORGANIZATIONS_PATH + ORGANIZATION_ID_PATH)
    fun getCollaborationsIncludingOrganization(@Path(ORGANIZATION_ID) organizationId: UUID): Iterable<Collaboration>

    @DELETE(BASE + ID_PATH)
    fun deleteCollaboration(@Path(ID) id: UUID)

    @POST(BASE + ID_PATH + ORGANIZATIONS_PATH)
    fun addOrganizationIdsToCollaboration(@Path(ID) id: UUID, @Body organizationIds: Set<UUID>)

    @DELETE(BASE + ID_PATH + ORGANIZATIONS_PATH)
    fun removeOrganizationIdsFromCollaboration(@Path(ID) id: UUID, @Body organizationIds: Set<UUID>)

    @GET(BASE + ID_PATH + DATABASE_PATH)
    fun getCollaborationDatabaseInfo(@Path(ID) id: UUID): OrganizationDatabase

    @PATCH(BASE + ID_PATH + DATABASE_PATH)
    fun renameDatabase(@Path(ID) id: UUID, @Body newDatabaseName: String)

    @GET(BASE + ID_PATH + PROJECT_PATH + ORGANIZATION_ID_PATH + TABLE_ID_PATH)
    fun projectTableToCollaboration(
            @Path(ID) collaborationId: UUID,
            @Path(ORGANIZATION_ID) organizationId: UUID,
            @Path(TABLE_ID) tableId: UUID
    )

    @DELETE(BASE + ID_PATH + PROJECT_PATH + ORGANIZATION_ID_PATH + TABLE_ID_PATH)
    fun removeProjectedTableFromCollaboration(
            @Path(ID) collaborationId: UUID,
            @Path(ORGANIZATION_ID) organizationId: UUID,
            @Path(TABLE_ID) tableId: UUID
    )
}