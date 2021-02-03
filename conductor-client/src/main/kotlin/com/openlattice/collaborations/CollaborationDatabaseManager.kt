package com.openlattice.collaborations

import com.openlattice.authorization.AclKey
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

    fun addMembersToOrganizationInCollaborations(collaborationIds: Set<UUID>, organizationId: UUID, members: Set<AclKey>)

    fun removeMembersFromOrganizationInCollaboration(
            collaborationId: UUID,
            organizationId: UUID,
            membersToRemoveFromSchema: Set<AclKey>,
            membersToRemoveFromDatabase: Set<AclKey>
    )
}