package com.openlattice.collaborations

import com.google.common.base.Preconditions
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.query.Predicate
import com.hazelcast.query.Predicates
import com.openlattice.authorization.*
import com.openlattice.authorization.securable.SecurableObjectType
import com.openlattice.collaborations.mapstores.CollaborationMapstore
import com.openlattice.collaborations.mapstores.ProjectedTablesMapstore
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.organizations.OrganizationDatabase
import com.openlattice.organizations.roles.SecurePrincipalsManager
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
    private val projectedTables = HazelcastMap.PROJECTED_TABLES.getMap(hazelcast)
    private val externalTables = HazelcastMap.ORGANIZATION_EXTERNAL_DATABASE_TABLE.getMap(hazelcast)

    companion object {
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

        authorizationManager.setSecurableObjectType(aclKey, SecurableObjectType.Collaboration)
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
        getCollaborationIdsIncludingOrg(organizationId).forEach {
            collaborationDatabaseManager.handleOrganizationDatabaseRename(it, organizationId, oldName, newName)
        }
    }

    fun handleOrganizationDeleted(organizationId: UUID) {
        getCollaborationIdsIncludingOrg(organizationId).forEach {
            removeOrganizationIdsFromCollaboration(it, setOf(organizationId))
        }
    }

    fun handleMembersAdddedToOrg(organizationId: UUID, newMembers: Set<AclKey>) {
        val collaborationIds = getCollaborationIdsIncludingOrg(organizationId)

        if (collaborationIds.isNotEmpty()) {
            collaborationDatabaseManager.addMembersToOrganizationInCollaborations(collaborationIds, organizationId, newMembers)
        }
    }

    fun handleMembersRemovedFromOrg(organizationId: UUID, removedMembers: Set<AclKey>) {
        val collaborationsIncludingOrg = getCollaborationsIncludingOrg(organizationId)

        if (collaborationsIncludingOrg.isEmpty()) {
            return
        }

        val membersByOrg = principalsManager.getOrganizationMembers(
                collaborationsIncludingOrg.flatMap { it.organizationIds }.toSet()
        )

        collaborationsIncludingOrg.forEach { collaboration ->
            val remainingMemberAclKeys = collaboration.organizationIds
                    .flatMap { membersByOrg[it] ?: setOf() }
                    .map { it.aclKey }
                    .toSet()

            val membersToRemoveFromDatabase = removedMembers.filter { !remainingMemberAclKeys.contains(it) }.toSet()

            collaborationDatabaseManager.removeMembersFromOrganizationInCollaboration(
                    collaboration.id,
                    organizationId,
                    removedMembers,
                    membersToRemoveFromDatabase
            )
        }
    }

    fun projectTableToCollaboration(collaborationId: UUID, organizationId: UUID, tableId: UUID) {
        ensureTableBelongsToOrganization(tableId, organizationId)

        val tableName = externalTables.getValue(tableId).name

        check(projectedTables.keySet(Predicates.and(
                collaborationIdPredicate(collaborationId),
                organizationIdPredicate(organizationId),
                tableNamePredicate(tableName)
        )).isEmpty()) {
            "Cannot project table $tableId because a table with name $tableName already exists in schema for " +
                    "organization $organizationId in collaboration $collaborationId database"
        }

        val key = ProjectedTableKey(tableId, collaborationId)
        val metadata = ProjectedTableMetadata(organizationId, tableName)

        check(projectedTables.putIfAbsent(key, metadata) == null) {
            "Table $tableId is already projected in collaboration $collaborationId"
        }

        collaborationDatabaseManager.initializeTableProjection(collaborationId, organizationId, tableId)
    }

    fun removeProjectedTableFromCollaboration(collaborationId: UUID, organizationId: UUID, tableId: UUID) {
        ensureTableBelongsToOrganization(tableId, organizationId)

        val key = ProjectedTableKey(tableId, collaborationId)
        val metadata = projectedTables[key]
        check(metadata != null) {
            "Table $tableId is not projected in collaboration $collaborationId"
        }

        collaborationDatabaseManager.removeTableProjection(collaborationId, organizationId, tableId)
        projectedTables.remove(key)
    }

    fun handleTableUpdate(tableId: UUID) {
        projectedTables.entrySet(tableIdPredicate(tableId)).forEach {
            collaborationDatabaseManager.refreshTableProjection(it.key.collaborationId, it.value.organizationId, tableId)
        }
    }

    fun <T> getProjectedTableIdsInCollaborationsAndOrganizations(
            collaborationIds: Collection<UUID>,
            organizationIds: Collection<UUID>,
            groupingFn: (Map.Entry<ProjectedTableKey, ProjectedTableMetadata>) -> T
    ): Map<T, List<UUID>> {
        return projectedTables.entrySet(Predicates.and(
                collaborationIdsPredicate(collaborationIds),
                organizationIdsPredicate(organizationIds)
        ))
                .groupBy { groupingFn(it) }
                .mapValues { entry -> entry.value.map { it.key.tableId } }

    }

    fun getCollaborationIdsWithProjectionsForTables(tableIds: Set<UUID>, collaborationIds: Set<UUID>): Map<UUID, List<UUID>> {
        return projectedTables.entrySet(Predicates.and(
                tableIdsPredicate(tableIds),
                collaborationIdsPredicate(collaborationIds)
        ))
                .groupBy { it.key.tableId }
                .mapValues { entry -> entry.value.map { it.key.collaborationId } }
    }

    private fun ensureTableBelongsToOrganization(tableId: UUID, organizationId: UUID) {
        check(externalTables.getValue(tableId).organizationId == organizationId) {
            "Table $tableId does not belong to organization $organizationId"
        }
    }

    fun getCollaborationsIncludingOrg(organizationId: UUID): Collection<Collaboration> {
        return collaborations.values(Predicates.equal(CollaborationMapstore.ORGANIZATION_ID_IDX, organizationId))
    }

    private fun getCollaborationIdsIncludingOrg(organizationId: UUID): Set<UUID> {
        return collaborations.keySet(Predicates.equal(CollaborationMapstore.ORGANIZATION_ID_IDX, organizationId))
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
        collaborations.executeOnKey(id, AlterOrganizationsInCollaborationEntryProcessor(organizationIds, true))

        collaborationDatabaseManager.addOrganizationsToCollaboration(id, organizationIds)
    }

    private fun removeReadOnCollaborationFromOrganizations(id: UUID, organizationIds: Set<UUID>) {
        if (organizationIds.isEmpty()) {
            return
        }

        authorizationManager.removePermissions(getOrgAcls(id, organizationIds))
        collaborations.executeOnKey(id, AlterOrganizationsInCollaborationEntryProcessor(organizationIds, false))

        collaborationDatabaseManager.removeOrganizationsFromCollaboration(id, organizationIds)
    }

    private fun getOrgAcls(id: UUID, organizationIds: Set<UUID>): List<Acl> {
        val orgAces = principalsManager
                .getSecurablePrincipals(organizationIds.map { AclKey(it) }.toSet())
                .values
                .map { Ace(it.principal, READ_PERMISSIONS) }

        return listOf(Acl(AclKey(id), orgAces))
    }

    private fun collaborationIdPredicate(collaborationId: UUID): Predicate<ProjectedTableKey, ProjectedTableMetadata> {
        return Predicates.equal(ProjectedTablesMapstore.COLLABORATION_ID_INDEX, collaborationId)
    }

    private fun collaborationIdsPredicate(collaborationIds: Collection<UUID>): Predicate<ProjectedTableKey, ProjectedTableMetadata> {
        return Predicates.`in`(ProjectedTablesMapstore.COLLABORATION_ID_INDEX, *collaborationIds.toTypedArray())
    }

    private fun organizationIdPredicate(organizationId: UUID): Predicate<ProjectedTableKey, ProjectedTableMetadata> {
        return Predicates.equal(ProjectedTablesMapstore.ORGANIZATION_ID_INDEX, organizationId)
    }

    private fun organizationIdsPredicate(organizationIds: Collection<UUID>): Predicate<ProjectedTableKey, ProjectedTableMetadata> {
        return Predicates.`in`(ProjectedTablesMapstore.ORGANIZATION_ID_INDEX, *organizationIds.toTypedArray())
    }

    private fun tableIdPredicate(tableId: UUID): Predicate<ProjectedTableKey, ProjectedTableMetadata> {
        return Predicates.equal(ProjectedTablesMapstore.TABLE_ID_INDEX, tableId)
    }

    private fun tableIdsPredicate(tableIds: Collection<UUID>): Predicate<ProjectedTableKey, ProjectedTableMetadata> {
        return Predicates.`in`(ProjectedTablesMapstore.TABLE_ID_INDEX, *tableIds.toTypedArray())
    }

    private fun tableNamePredicate(tableName: String): Predicate<ProjectedTableKey, ProjectedTableMetadata> {
        return Predicates.equal(ProjectedTablesMapstore.TABLE_NAME_INDEX, tableName)
    }

}
