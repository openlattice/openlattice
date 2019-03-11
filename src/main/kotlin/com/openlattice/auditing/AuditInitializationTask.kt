package com.openlattice.auditing

import com.google.common.collect.ImmutableSet
import com.hazelcast.core.HazelcastInstance
import com.openlattice.assembler.tasks.UsersAndRolesInitializationTask
import com.openlattice.authorization.AclKey
import com.openlattice.authorization.Permission
import com.openlattice.authorization.SystemRole
import com.openlattice.edm.EntitySet
import com.openlattice.edm.set.EntitySetFlag
import com.openlattice.edm.type.EntityType
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.organizations.tasks.OrganizationsInitializationTask
import com.openlattice.tasks.HazelcastInitializationTask
import com.openlattice.tasks.PostConstructInitializerTaskDependencies.PostConstructInitializerTask
import com.openlattice.tasks.Task
import com.openlattice.users.Auth0SyncInitializationTask
import com.openlattice.users.Auth0SyncTask
import org.slf4j.LoggerFactory
import java.util.*

private val logger = LoggerFactory.getLogger(AuditInitializationTask::class.java)

class AuditInitializationTask(
        val hazelcastInstance: HazelcastInstance
) : HazelcastInitializationTask<AuditTaskDependencies> {

    private val entityTypes = hazelcastInstance.getMap<UUID, EntityType>(HazelcastMap.ENTITY_TYPES.name)
    private val entitySets = hazelcastInstance.getMap<UUID, EntitySet>(HazelcastMap.ENTITY_SETS.name)
    private val organizations = hazelcastInstance.getMap<UUID, String>(HazelcastMap.ORGANIZATIONS_TITLES.name)
    private val auditRecordEntitySetConfigurations = hazelcastInstance.getMap<AclKey, AuditRecordEntitySetConfiguration>(
            HazelcastMap.AUDIT_RECORD_ENTITY_SETS.name
    )

    override fun getInitialDelay(): Long {
        return 0
    }

    override fun initialize(dependencies: AuditTaskDependencies) {
        if (entityTypes.isEmpty) {
            logger.info("EDM not yet initialized -- skipping audit initialization.")
            return
        }
        logger.info("Creating any missing audit entity sets")
        ensureEdmEntitySetExists(dependencies)
        ensureAllEntitySetsHaveAuditEntitySets(dependencies)
        ensureAllOrganizationsHaveAuditEntitySets(dependencies)
    }

    override fun after(): Set<Class<out HazelcastInitializationTask<*>>> {
        return setOf(
                PostConstructInitializerTask::class.java,
                UsersAndRolesInitializationTask::class.java,
                OrganizationsInitializationTask::class.java,
                Auth0SyncInitializationTask::class.java
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
        var edmAuditEntitySet = dependencies.edmService.getEntitySet(EDM_AUDIT_ENTITY_SET_NAME)

        if (edmAuditEntitySet == null) {

            val admins = dependencies.spm.getAllUsersWithPrincipal(dependencies.spm.lookup(SystemRole.ADMIN.principal))

            if (dependencies.edmService.auditRecordEntitySetsManager.auditingTypes.isAuditingInitialized()) {

                edmAuditEntitySet = EntitySet(
                        dependencies.edmService.auditRecordEntitySetsManager.auditingTypes.auditingEntityTypeId,
                        EDM_AUDIT_ENTITY_SET_NAME,
                        "EDM Audit Entity Set",
                        Optional.of("Audit entity set for the entity data model"),
                        ImmutableSet.of(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.of(EnumSet.of(EntitySetFlag.AUDIT)))

                dependencies.edmService.createEntitySet(admins.first(), edmAuditEntitySet)
            }

            val edmAuditAclKeys = dependencies.edmService.auditRecordEntitySetsManager.auditingTypes.propertyTypeIds.values.map { AclKey(edmAuditEntitySet.id, it) }.toMutableSet()
            edmAuditAclKeys.add(AclKey(edmAuditEntitySet.id))

            dependencies.authorizationManager.setPermission(
                    edmAuditAclKeys,
                    setOf(SystemRole.ADMIN.principal),
                    EnumSet.allOf(Permission::class.java))

        }
    }

    private fun ensureAllEntitySetsHaveAuditEntitySets(dependencies: AuditTaskDependencies) {
        entitySets.entries
                .filter { !auditRecordEntitySetConfigurations.keys.contains(AclKey(it.key)) && !it.value.flags.contains(EntitySetFlag.AUDIT) }
                .forEach { dependencies.edmService.auditRecordEntitySetsManager.createAuditEntitySetForEntitySet(it.value) }
    }

    private fun ensureAllOrganizationsHaveAuditEntitySets(dependencies: AuditTaskDependencies) {
        organizations.keys
                .filter { !auditRecordEntitySetConfigurations.keys.contains(AclKey(it)) }
                .forEach { dependencies.edmService.auditRecordEntitySetsManager.createAuditEntitySetForOrganization(it) }
    }

}