package com.openlattice.collaborations

import com.google.common.base.Preconditions
import com.hazelcast.core.HazelcastInstance
import com.openlattice.assembler.Assembler
import com.openlattice.authorization.*
import com.openlattice.authorization.securable.SecurableObjectType
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.organizations.OrganizationDatabase
import com.openlattice.organizations.roles.SecurePrincipalsManager
import org.slf4j.LoggerFactory
import java.util.*

class CollaborationService(
        hazelcast: HazelcastInstance,
        private val aclKeyReservationService: HazelcastAclKeyReservationService,
        private val authorizationManager: AuthorizationManager,
        private val principalsManager: SecurePrincipalsManager,
        private val collaborationDatabaseManager: CollaborationDatabaseManager
) {

    private val collaborations = HazelcastMap.COLLABORATIONS.getMap(hazelcast)
    private val organizations = HazelcastMap.ORGANIZATIONS.getMap(hazelcast)

    companion object {
        private val logger = LoggerFactory.getLogger(CollaborationService::class.java)
        private val READ_PERMISSIONS = EnumSet.of(Permission.READ)
    }

    fun getCollaborations(ids: Set<UUID>): Map<UUID, Collaboration> {
        return collaborations.getAll(ids)

    }

    fun getCollaboration(id: UUID): Collaboration {
        return collaborations.getValue(id)
    }

    fun createCollaboration(collaboration: Collaboration, ownerPrincipal: Principal): UUID {
        ensureValidOrganizationIds(collaboration.organizationIds)

        val aclKey = reserveCollaborationIfNotExists(collaboration)

        authorizationManager.setSecurableObjectType(aclKey, SecurableObjectType.EntitySet)
        authorizationManager.addPermission(aclKey, ownerPrincipal, EnumSet.allOf(Permission::class.java))

        collaborationDatabaseManager.createCollaborationDatabase(collaboration.id)
        grantReadOnCollaborationToOrganizations(collaboration.id, collaboration.organizationIds)

        return collaboration.id
    }

    fun deleteCollaboration(id: UUID) {
        ensureValidCollaborationId(id)

        authorizationManager.deletePermissions(AclKey(id))
        collaborations.delete(id)
        aclKeyReservationService.release(id)

        collaborationDatabaseManager.deleteCollaborationDatabase(id)
    }

    fun addOrganizationIdsToCollaboration(id: UUID, organizationIds: Set<UUID>) {
        ensureValidCollaborationId(id)
        grantReadOnCollaborationToOrganizations(id, organizationIds)
    }

    fun removeOrganizationIdsFromCollaboration(id: UUID, organizationIds: Set<UUID>) {
        ensureValidCollaborationId(id)

        removeReadOnCollaborationFromOrganizations(id, organizationIds)
    }

    fun getDatabaseInfo(id: UUID): OrganizationDatabase {
        return collaborationDatabaseManager.getDatabaseInfo(id)
    }

    fun renameDatabase(id: UUID, newName: String) {
        collaborationDatabaseManager.renameCollaborationDatabase(id, newName)
    }

    fun handleOrganizationDatabaseRename(organizationId: UUID, oldName: String, newName: String) {
        collaborationDatabaseManager.handleOrganizationDatabaseRename(organizationId, oldName, newName)
    }

    private fun reserveCollaborationIfNotExists(collaboration: Collaboration): AclKey {
        aclKeyReservationService.reserveIdAndValidateType(collaboration) { collaboration.name }

        Preconditions.checkState(collaborations.putIfAbsent(collaboration.id, collaboration) == null, "Collaboration already exists.")
        return AclKey(collaboration.id)
    }

    private fun ensureValidCollaborationId(id: UUID) {
        check(collaborations.containsKey(id)) { "No collaboration exists with id $id" }
    }

    private fun ensureValidOrganizationIds(organizationIds: Set<UUID>) {
        if (organizationIds.isEmpty()) {
            return
        }

        val existingOrgIds = organizations.getAll(organizationIds).keys
        val nonexistentOrgIds = organizationIds.filter { !existingOrgIds.contains(it) }

        check(nonexistentOrgIds.isEmpty()) { "The following organization ids do not exist: $nonexistentOrgIds" }
    }

    private fun grantReadOnCollaborationToOrganizations(id: UUID, organizationIds: Set<UUID>) {
        if (organizationIds.isEmpty()) {
            return
        }

        ensureValidOrganizationIds(organizationIds)

        authorizationManager.addPermissions(getOrgAcls(id, organizationIds))
        collaborations.executeOnKey(id, CollaborationEntryProcessor {
            it.addOrganizationIds(organizationIds)
            CollaborationEntryProcessor.Result()
        })

        collaborationDatabaseManager.addOrganizationsToCollaboration(id, organizationIds)
    }

    private fun removeReadOnCollaborationFromOrganizations(id: UUID, organizationIds: Set<UUID>) {
        if (organizationIds.isEmpty()) {
            return
        }

        authorizationManager.removePermissions(getOrgAcls(id, organizationIds))
        collaborations.executeOnKey(id, CollaborationEntryProcessor {
            it.removeOrganizationIds(organizationIds)
            CollaborationEntryProcessor.Result()
        })

        collaborationDatabaseManager.removeOrganizationsFromCollaboration(id, organizationIds)
    }

    private fun getOrgAcls(id: UUID, organizationIds: Set<UUID>): List<Acl> {
        val orgAces = principalsManager
                .getSecurablePrincipals(organizationIds.map { AclKey(it) }.toSet())
                .values
                .map { Ace(it.principal, READ_PERMISSIONS) }

        return listOf(Acl(AclKey(id), orgAces))
    }


}