package com.openlattice.auditing

import com.hazelcast.core.HazelcastInstance
import com.openlattice.IdConstants
import com.openlattice.assembler.tasks.UsersAndRolesInitializationTask
import com.openlattice.authorization.AclKey
import com.openlattice.authorization.Permission
import com.openlattice.authorization.SystemRole
import com.openlattice.edm.EntitySet
import com.openlattice.edm.set.EntitySetFlag
import com.openlattice.edm.tasks.EdmSyncInitializerTask
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.organizations.tasks.OrganizationsInitializationTask
import com.openlattice.tasks.HazelcastInitializationTask
import com.openlattice.tasks.PostConstructInitializerTaskDependencies.PostConstructInitializerTask
import com.openlattice.tasks.Task
import com.openlattice.users.Auth0SyncInitializationTask
import org.slf4j.LoggerFactory
import java.util.*

private val logger = LoggerFactory.getLogger(AuditInitializationTask::class.java)

class AuditInitializationTask(
        val hazelcastInstance: HazelcastInstance
) : HazelcastInitializationTask<AuditTaskDependencies> {

    private val entityTypes = HazelcastMap.ENTITY_TYPES.getMap(hazelcastInstance)
    private val entitySets = HazelcastMap.ENTITY_SETS.getMap(hazelcastInstance)
    private val organizations = HazelcastMap.ORGANIZATIONS.getMap(hazelcastInstance)
    private val auditRecordEntitySetConfigurations = HazelcastMap.AUDIT_RECORD_ENTITY_SETS.getMap(hazelcastInstance)

    override fun getInitialDelay(): Long {
        return 0
    }

    override fun initialize(dependencies: AuditTaskDependencies) {
        if (entityTypes.isEmpty) {
            logger.info("EDM not yet initialized -- skipping audit initialization.")
            return
        }
        dependencies.entitySetManager.getAuditRecordEntitySetsManager().auditingTypes.intialize()
        logger.info("Creating any missing audit entity sets")
        val auditedAclKeys = auditRecordEntitySetConfigurations.keys.toSet()
        ensureEdmEntitySetExists(dependencies)
        ensureAllEntitySetsHaveAuditEntitySets(dependencies, auditedAclKeys)
        ensureAllEntitySetsHaveAuditEdgeEntitySets(dependencies)
        ensureAllOrganizationsHaveAuditEntitySets(dependencies, auditedAclKeys)
    }

    override fun after(): Set<Class<out HazelcastInitializationTask<*>>> {
        return setOf(
                PostConstructInitializerTask::class.java,
                UsersAndRolesInitializationTask::class.java,
                OrganizationsInitializationTask::class.java,
                Auth0SyncInitializationTask::class.java,
                EdmSyncInitializerTask::class.java
        )
    }

    override fun isRunOnceAcrossCluster(): Boolean {
        return false
    }

    override fun getName(): String {
        return Task.AUDIT_INITIALIZATION.name
    }

    override fun getDependenciesClass(): Class<out AuditTaskDependencies> {
        return AuditTaskDependencies::class.java
    }

    private fun ensureEdmEntitySetExists(dependencies: AuditTaskDependencies) {
        var edmAuditEntitySet = dependencies.entitySetManager.getEntitySet(EDM_AUDIT_ENTITY_SET_NAME)

        if (edmAuditEntitySet == null) {

            if (dependencies.entitySetManager.getAuditRecordEntitySetsManager().auditingTypes.isAuditingInitialized()) {

                val adminRoleAclKey = dependencies.organizationsManager.getAdminRoleAclKey(IdConstants.GLOBAL_ORGANIZATION_ID.id)
                val adminRolePrincipal = dependencies.spm.getSecurablePrincipal(adminRoleAclKey).principal

                edmAuditEntitySet = EntitySet(
                        entityTypeId = dependencies.entitySetManager.getAuditRecordEntitySetsManager().auditingTypes
                                .auditingEntityTypeId,
                        name = EDM_AUDIT_ENTITY_SET_NAME,
                        _title = "EDM Audit Entity Set",
                        _description = "Audit entity set for the entity data model",
                        contacts = mutableSetOf(),
                        organizationId = IdConstants.GLOBAL_ORGANIZATION_ID.id,
                        flags = EnumSet.of(EntitySetFlag.AUDIT),
                        partitions = LinkedHashSet(dependencies.partitionManager.getAllPartitions())
                )

                dependencies.entitySetManager.createEntitySet(adminRolePrincipal, edmAuditEntitySet)
            }

            val edmAuditAclKeys = dependencies.entitySetManager.getAuditRecordEntitySetsManager().auditingTypes
                    .propertyTypeIds.values.map {
                AclKey(edmAuditEntitySet!!.id, it)
            }.toMutableSet()
            edmAuditAclKeys.add(AclKey(edmAuditEntitySet!!.id))

            dependencies.authorizationManager.setPermission(
                    edmAuditAclKeys,
                    setOf(SystemRole.ADMIN.principal),
                    EnumSet.allOf(Permission::class.java)
            )

        }
    }

    private fun ensureAllEntitySetsHaveAuditEntitySets(
            dependencies: AuditTaskDependencies, auditedAclKeys: Set<AclKey>
    ) {
        entitySets.entries
                .filter {
                    !auditedAclKeys.contains(AclKey(it.key)) && !it.value.flags.contains(
                            EntitySetFlag.AUDIT
                    )
                }
                .forEach {
                    dependencies.entitySetManager.getAuditRecordEntitySetsManager().createAuditEntitySetForEntitySet(
                            it.value
                    )
                }
    }

    private fun ensureAllEntitySetsHaveAuditEdgeEntitySets(dependencies: AuditTaskDependencies) {
        auditRecordEntitySetConfigurations.entries
                .filter { it.value.activeAuditEdgeEntitySetId == null }
                .forEach { (entitySetId, _) ->
                    logger.info("Creating missing audit edge entity set for entity set {}", entitySetId)
                    dependencies.entitySetManager.getAuditRecordEntitySetsManager()
                            .initializeAuditEdgeEntitySet(entitySetId)
                }

    }

    private fun ensureAllOrganizationsHaveAuditEntitySets(
            dependencies: AuditTaskDependencies, auditedAclKeys: Set<AclKey>
    ) {
        organizations.keys
                .filter { !auditedAclKeys.contains(AclKey(it)) }
                .forEach {
                    dependencies.entitySetManager.getAuditRecordEntitySetsManager()
                            .createAuditEntitySetForOrganization(it)
                }
    }

}