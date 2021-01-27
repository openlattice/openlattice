package com.openlattice.collaborations

import com.openlattice.organizations.OrganizationDatabase
import java.util.*

interface CollaborationDatabaseManager {

    fun createCollaborationDatabase(collaboration: Collaboration)

    fun deleteCollaborationDatabase(collaborationId: UUID)

    fun renameCollaborationDatabase(collaborationId: UUID, newName: String)

    fun addOrganizationsToCollaboration(collaborationId: UUID, organizationIds: Set<UUID>)

    fun getDatabaseInfo(collaborationId: UUID): OrganizationDatabase

    fun removeOrganizationsFromCollaboration(collaborationId: UUID, organizationIds: Set<UUID>)
}