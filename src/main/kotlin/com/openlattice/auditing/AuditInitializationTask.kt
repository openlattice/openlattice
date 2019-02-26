package com.openlattice.auditing

import com.google.common.collect.ImmutableSet
import com.hazelcast.core.HazelcastInstance
import com.openlattice.assembler.tasks.UsersAndRolesInitializationTask
import com.openlattice.authorization.AclKey
import com.openlattice.authorization.Permission
import com.openlattice.authorization.SystemRole
import com.openlattice.edm.EntitySet
import com.openlattice.edm.set.EntitySetFlag
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.organizations.OrganizationsInitializationTask
import com.openlattice.tasks.HazelcastInitializationTask
import com.openlattice.tasks.PostConstructInitializerTaskDependencies.PostConstructInitializerTask
import com.openlattice.tasks.Task
import org.slf4j.LoggerFactory
import java.util.*

private val logger = LoggerFactory.getLogger(AuditInitializationTask::class.java)

class AuditInitializationTask(
        val hazelcastInstance: HazelcastInstance
) : HazelcastInitializationTask<AuditTaskDependencies> {

    private val entitySets = hazelcastInstance.getMap<UUID, EntitySet>(HazelcastMap.ENTITY_SETS.name)
    private val organizations = hazelcastInstance.getMap<UUID, String>(HazelcastMap.ORGANIZATIONS_TITLES.name)
    private val auditRecordEntitySetConfigurations = hazelcastInstance.getMap<AclKey, AuditRecordEntitySetConfiguration>(
            HazelcastMap.AUDIT_RECORD_ENTITY_SETS.name
    )

    override fun getInitialDelay(): Long {
        return 0
    }

    override fun initialize(dependencies: AuditTaskDependencies) {
        logger.info("Creating any missing audit entity sets")
        ensureEdmEntitySetExists(dependencies)
        ensureAllEntitySetsHaveAuditEntitySets(dependencies)
        ensureAllOrganizationsHaveAuditEntitySets(dependencies)
    }

    override fun after(): Set<Class<out HazelcastInitializationTask<*>>> {
        return setOf(
                PostConstructInitializerTask::class.java,
                UsersAndRolesInitializationTask::class.java,
                OrganizationsInitializationTask::class.java
        )
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

                dependencies.edmService.createEntitySet(admins.first(), EntitySet(
                        dependencies.edmService.auditRecordEntitySetsManager.auditingTypes.auditingEntityTypeId,
                        EDM_AUDIT_ENTITY_SET_NAME,
                        "EDM Audit Entity Set",
                        Optional.of("Audit entity set for the entity data model"),
                        ImmutableSet.of(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.of(EnumSet.of(EntitySetFlag.AUDIT))
                ))
            }

            edmAuditEntitySet = dependencies.edmService.getEntitySet(EDM_AUDIT_ENTITY_SET_NAME)

            dependencies.authorizationManager.addPermission(
                    AclKey(edmAuditEntitySet.id),
                    SystemRole.ADMIN.principal,
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