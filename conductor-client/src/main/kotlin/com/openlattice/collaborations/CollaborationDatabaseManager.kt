package com.openlattice.collaborations

import com.openlattice.organizations.OrganizationDatabase
import java.util.*

interface CollaborationDatabaseManager {

    fun createCollaborationDatabase(collaborationId: UUID)

    fun deleteCollaborationDatabase(collaborationId: UUID)

    fun renameCollaborationDatabase(collaborationId: UUID, newName: String)

    fun getDatabaseInfo(collaborationId: UUID): OrganizationDatabase

    fun addOrganizationsToCollaboration(collaborationId: UUID, organizationIds: Set<UUID>)

    fun removeOrganizationsFromCollaboration(collaborationId: UUID, organizationIds: Set<UUID>)

    fun handleOrganizationDatabaseRename(organizationId: UUID, oldName: String, newName: String)
}