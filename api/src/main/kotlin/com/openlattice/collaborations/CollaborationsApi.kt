package com.openlattice.collaborations

import retrofit2.http.*
import java.util.*

interface CollaborationsApi {

    companion object {
        // @formatter:off
        const val  SERVICE = "/datastore"
        const val  CONTROLLER = "/collaborations"
        const val  BASE = SERVICE + CONTROLLER
        // @formatter:on

        const val ORGANIZATIONS_PATH = "/organizations"

        const val ID = "id"
        const val ID_PATH = "/{$ID}"
    }

    @GET(BASE)
    fun getCollaborations(): Iterable<Collaboration>

    @POST(BASE)
    fun createCollaboration(@Body collaboration: Collaboration): UUID

    @GET(BASE + ID_PATH)
    fun getCollaboration(@Path(ID) id: UUID): Collaboration

    @DELETE(BASE + ID_PATH)
    fun deleteCollaboration(@Path(ID) id: UUID)

    @POST(BASE + ID_PATH + ORGANIZATIONS_PATH)
    fun addOrganizationIdsToCollaboration(@Path(ID) id: UUID, @Body organizationIds: Set<UUID>)

    @DELETE(BASE + ID_PATH + ORGANIZATIONS_PATH)
    fun removeOrganizationIdsFromCollaboration(@Path(ID) id: UUID, @Body organizationIds: Set<UUID>)
}