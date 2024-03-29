/*
 * Copyright (C) 2019. OpenLattice, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@openlattice.com
 *
 *
 */

package com.openlattice.assembler

import com.codahale.metrics.MetricRegistry
import com.codahale.metrics.MetricRegistry.name
import com.google.common.eventbus.EventBus
import com.google.common.eventbus.Subscribe
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.query.Predicate
import com.hazelcast.query.Predicates
import com.hazelcast.query.QueryConstants
import com.openlattice.assembler.events.MaterializePermissionChangeEvent
import com.openlattice.assembler.events.MaterializedEntitySetDataChangeEvent
import com.openlattice.assembler.events.MaterializedEntitySetEdmChangeEvent
import com.openlattice.assembler.processors.AddFlagsToMaterializedEntitySetProcessor
import com.openlattice.assembler.processors.AddFlagsToOrganizationMaterializedEntitySetProcessor
import com.openlattice.assembler.processors.UpdateRefreshRateProcessor
import com.openlattice.authorization.AclKey
import com.openlattice.authorization.AuthorizationManager
import com.openlattice.authorization.DbCredentialService
import com.openlattice.authorization.EdmAuthorizationHelper
import com.openlattice.authorization.securable.SecurableObjectType
import com.geekbeast.controllers.exceptions.ResourceNotFoundException
import com.openlattice.directory.MaterializedViewAccount
import com.openlattice.edm.events.EntitySetDeletedEvent
import com.openlattice.edm.events.EntitySetNameUpdatedEvent
import com.openlattice.edm.events.EntitySetOrganizationUpdatedEvent
import com.openlattice.edm.type.PropertyType
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.organization.OrganizationEntitySetFlag
import com.openlattice.organization.OrganizationIntegrationAccount
import com.openlattice.organizations.OrganizationDatabase
import com.openlattice.organizations.events.MembersAddedToOrganizationEvent
import com.openlattice.organizations.events.MembersRemovedFromOrganizationEvent
import com.openlattice.organizations.roles.SecurePrincipalsManager
import com.openlattice.organizations.tasks.OrganizationsInitializationTask
import com.openlattice.postgres.external.DatabaseQueryManager
import com.openlattice.postgres.mapstores.MaterializedEntitySetMapStore
import com.openlattice.postgres.mapstores.OrganizationAssemblyMapstore
import com.openlattice.postgres.mapstores.OrganizationAssemblyMapstore.INITIALIZED_INDEX
import com.geekbeast.tasks.HazelcastInitializationTask
import com.geekbeast.tasks.HazelcastTaskDependencies
import com.geekbeast.tasks.PostConstructInitializerTaskDependencies.PostConstructInitializerTask
import com.geekbeast.tasks.Task
import org.slf4j.LoggerFactory
import java.util.*
import java.util.stream.Collectors
import kotlin.streams.toList

private val logger = LoggerFactory.getLogger(Assembler::class.java)

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class Assembler(
        private val dbCredentialService: DbCredentialService,
        private val authorizationManager: AuthorizationManager,
        private val securePrincipalsManager: SecurePrincipalsManager,
        val dbQueryManager: DatabaseQueryManager,
        metricRegistry: MetricRegistry,
        hazelcast: HazelcastInstance,
        eventBus: EventBus
) : HazelcastTaskDependencies {

    private val entitySets = HazelcastMap.ENTITY_SETS.getMap(hazelcast)
    private val propertyTypes = HazelcastMap.PROPERTY_TYPES.getMap(hazelcast)
    private val assemblies = HazelcastMap.ASSEMBLIES.getMap(hazelcast)
    private val materializedEntitySets = HazelcastMap.MATERIALIZED_ENTITY_SETS.getMap(hazelcast)
    private val securableObjectTypes = HazelcastMap.SECURABLE_OBJECT_TYPES.getMap(hazelcast)
    private val principals = HazelcastMap.PRINCIPALS.getMap(hazelcast)
    val organizationDatabases = HazelcastMap.ORGANIZATION_DATABASES.getMap(hazelcast)

    private val deleteOrganizationTimer = metricRegistry.timer(name(Assembler::class.java, "deleteOrganization"))

    init {
        eventBus.register(this)
    }

    fun getMaterializedEntitySetsInOrganization(organizationId: UUID): Map<UUID, Set<OrganizationEntitySetFlag>> {
        return assemblies[organizationId]?.materializedEntitySets ?: mapOf()
    }

    fun getMaterializedEntitySetIdsInOrganization(organizationId: UUID): Set<UUID> {
        return assemblies[organizationId]?.materializedEntitySets?.keys ?: setOf()
    }

    @Subscribe
    fun handleEntitySetDataChange(entitySetDataChangeEvent: MaterializedEntitySetDataChangeEvent) {
        flagMaterializedEntitySet(entitySetDataChangeEvent.entitySetId, OrganizationEntitySetFlag.DATA_UNSYNCHRONIZED)
    }

    @Subscribe
    fun handleEntitySetEdmChange(entitySetEdmChangeEvent: MaterializedEntitySetEdmChangeEvent) {
        flagMaterializedEntitySet(entitySetEdmChangeEvent.entitySetId, OrganizationEntitySetFlag.EDM_UNSYNCHRONIZED)
    }

    private fun flagMaterializedEntitySet(entitySetId: UUID, flag: OrganizationEntitySetFlag) {
        if (isEntitySetMaterialized(entitySetId)) {
            materializedEntitySets.executeOnEntries(
                    AddFlagsToMaterializedEntitySetProcessor(setOf(flag)),
                    entitySetIdPredicate(entitySetId)
            )
            assemblies.executeOnEntries(
                    AddFlagsToOrganizationMaterializedEntitySetProcessor(entitySetId, setOf(flag)),
                    entitySetIdInOrganizationPredicate(entitySetId) as Predicate<UUID, OrganizationAssembly>
            )
        }
    }

    @Subscribe
    fun handleMaterializePermissionChange(materializePermissionChangeEvent: MaterializePermissionChangeEvent) {
        val organizationId = securePrincipalsManager.lookup(materializePermissionChangeEvent.organizationPrincipal)[0]

        flagMaterializedEntitySetWithPermissionChange(
                organizationId,
                materializePermissionChangeEvent.entitySetIds,
                materializePermissionChangeEvent.objectType
        )
    }

    private fun flagMaterializedEntitySetWithPermissionChange(
            organizationId: UUID,
            entitySetIds: Set<UUID>,
            objectType: SecurableObjectType
    ) {
        val entitySetAssemblyKeys = entitySetIds
                .map { EntitySetAssemblyKey(it, organizationId) }
                .filter { isEntitySetMaterialized(it) }
                .toSet()

        val flagToAdd = if (objectType == SecurableObjectType.EntitySet) {
            OrganizationEntitySetFlag.MATERIALIZE_PERMISSION_REMOVED
        } else {
            OrganizationEntitySetFlag.MATERIALIZE_PERMISSION_UNSYNCHRONIZED
        }
        materializedEntitySets.executeOnKeys(
                entitySetAssemblyKeys,
                AddFlagsToMaterializedEntitySetProcessor(setOf(flagToAdd))
        )
    }

    /**
     * Updates the refresh rate for a materialized entity set.
     */
    fun updateRefreshRate(organizationId: UUID, entitySetId: UUID, refreshRate: Long?) {
        val entitySetAssemblyKey = EntitySetAssemblyKey(entitySetId, organizationId)

        ensureEntitySetMaterialized(entitySetAssemblyKey)

        materializedEntitySets.executeOnKey(entitySetAssemblyKey, UpdateRefreshRateProcessor(refreshRate))
    }

    @Subscribe
    fun handleEntitySetDeleted(entitySetDeletedEvent: EntitySetDeletedEvent) {
        // TODO - Transporter
    }

    @Subscribe
    fun handleEntitySetOrganizationUpdated(entitySetOrganizationUpdatedEvent: EntitySetOrganizationUpdatedEvent) {
        // TODO - Transporter
    }

    @Subscribe
    fun handleEntitySetNameUpdated(entitySetNameUpdatedEvent: EntitySetNameUpdatedEvent) {
        // TODO - Transporter
    }

    fun createOrganizationAndReturnOid(organizationId: UUID): OrganizationDatabase {
        return createOrganization(organizationId)
    }

    fun createOrganization(organizationId: UUID): OrganizationDatabase {
        return dbQueryManager.createAndInitializeOrganizationDatabase(organizationId)
    }

    fun renameDatabase(currentDatabaseName: String, newDatabaseName: String) {
        dbQueryManager.renameDatabase(currentDatabaseName, newDatabaseName)
    }

    fun destroyOrganization(organizationId: UUID) {
        deleteOrganizationTimer.time().use {
            assemblies.delete(organizationId)
            materializedEntitySets.removeAll(organizationIdPredicate(organizationId))
        }
    }

    @Subscribe
    fun handleAddMembersToOrganization(event: MembersAddedToOrganizationEvent) {
        // check if organization is initialized
        ensureAssemblyInitialized(event.organizationId)

        val authorizedPropertyTypesOfEntitySetsByNewMembers = event.newMembers.associateWith { principal ->
            // we also grant select on entity sets, where no property type is authorized
            val authorizedEntitySets = authorizationManager.getAuthorizedObjectsOfType(
                    principal.principal,
                    SecurableObjectType.EntitySet,
                    EdmAuthorizationHelper.READ_PERMISSION
            )
            val authorizedPropertyTypeAcls = authorizationManager.getAuthorizedObjectsOfType(
                    principal.principal,
                    SecurableObjectType.PropertyTypeInEntitySet,
                    EdmAuthorizationHelper.READ_PERMISSION
            )

            val allEntitySets = entitySets
                    .getAll(authorizedEntitySets.map { it[0] }.collect(Collectors.toSet()))
                    .filter { isEntitySetMaterialized(EntitySetAssemblyKey(it.key, event.organizationId)) }
            val authorizedPropertyTypesByEntitySets = authorizedPropertyTypeAcls.toList()
                    .groupBy { it[0] }
                    .map { it.key to it.value.map { ak -> ak[1] } }
                    .toMap()

            allEntitySets.values
                    .associateWith {
                        if (authorizedPropertyTypesByEntitySets.containsKey(it.id)) {
                            propertyTypes.getAll(authorizedPropertyTypesByEntitySets.getValue(it.id).toSet()).values
                        } else {
                            listOf<PropertyType>()
                        }
                    }
        }

        dbQueryManager.addMembersToOrganization(event.organizationId, authorizedPropertyTypesOfEntitySetsByNewMembers)
    }

    @Subscribe
    fun removeMembersFromOrganization(event: MembersRemovedFromOrganizationEvent) {
        // check if organization is initialized
        ensureAssemblyInitialized(event.organizationId)

        dbQueryManager.removeMembersFromOrganization(event.organizationId, event.members)
    }

    fun getOrganizationIntegrationAccount(organizationId: UUID): OrganizationIntegrationAccount {
        val account = this.dbCredentialService.getDbAccount(AclKey(organizationId))
                ?: throw ResourceNotFoundException("Organization credential not found.")
        return OrganizationIntegrationAccount(account.username, account.credential)
    }

    fun rollIntegrationAccount(aclKey: AclKey): MaterializedViewAccount {
        val externalDatabaseId = dbCredentialService.getDbUsername(aclKey)
        val credential = dbCredentialService.rollCredential(aclKey)
        dbQueryManager.updateCredentialInDatabase(externalDatabaseId, credential)
        return MaterializedViewAccount(externalDatabaseId, credential)
    }

    /**
     * Returns true, if the entity set is materialized in any of the organization assemblies
     */
    private fun isEntitySetMaterialized(entitySetId: UUID): Boolean {
        return materializedEntitySets
                .keySet(entitySetIdPredicate(entitySetId))
                .isNotEmpty()
    }

    /**
     * Returns true, if the entity set is materialized in the organization
     */
    private fun isEntitySetMaterialized(entitySetAssemblyKey: EntitySetAssemblyKey): Boolean {
        return materializedEntitySets.keySet(entitySetAssemblyKeyPredicate(entitySetAssemblyKey)).isNotEmpty()
    }

    private fun ensureEntitySetMaterialized(entitySetAssemblyKey: EntitySetAssemblyKey) {
        if (!isEntitySetMaterialized(entitySetAssemblyKey)) {
            throw IllegalStateException(
                    "Entity set ${entitySetAssemblyKey.entitySetId} is not materialized for " +
                            "organization ${entitySetAssemblyKey.organizationId}"
            )
        }
    }

    private fun ensureAssemblyInitialized(organizationId: UUID) {
        val isAssemblyInitialized = organizationDatabases.containsKey(organizationId)
        if (!isAssemblyInitialized) {
            throw IllegalStateException("Organization assembly is not initialized for organization $organizationId")
        }
    }

    private fun entitySetIdPredicate(entitySetId: UUID): Predicate<EntitySetAssemblyKey, MaterializedEntitySet> {
        return Predicates.equal<EntitySetAssemblyKey, MaterializedEntitySet>(
                MaterializedEntitySetMapStore.ENTITY_SET_ID_INDEX,
                entitySetId
        )
    }

    @Suppress("UNCHECKED_CAST")
    private fun organizationIdPredicate(entitySetId: UUID): Predicate<EntitySetAssemblyKey, MaterializedEntitySet> {
        return Predicates.equal<EntitySetAssemblyKey, MaterializedEntitySet>(
                MaterializedEntitySetMapStore.ORGANIZATION_ID_INDEX,
                entitySetId
        )
                as Predicate<EntitySetAssemblyKey, MaterializedEntitySet>
    }

    private fun entitySetIdInOrganizationPredicate(entitySetId: UUID): Predicate<UUID, OrganizationAssembly> {
        return Predicates.equal<UUID, OrganizationAssembly>(
                OrganizationAssemblyMapstore.MATERIALIZED_ENTITY_SETS_ID_INDEX,
                entitySetId
        )
    }

    private fun entitySetAssemblyKeyPredicate(entitySetAssemblyKey: EntitySetAssemblyKey): Predicate<EntitySetAssemblyKey, MaterializedEntitySet> {
        return Predicates.equal<EntitySetAssemblyKey, MaterializedEntitySet>(
                QueryConstants.KEY_ATTRIBUTE_NAME.value(),
                entitySetAssemblyKey
        )
    }

    /**
     * This class is responsible for refreshing all entity set views at startup.
     */
    class EntitySetViewsInitializerTask : HazelcastInitializationTask<Assembler> {
        override fun getInitialDelay(): Long {
            return 0
        }

        override fun initialize(dependencies: Assembler) {
            // noop, this is now done in transporter
//            dependencies.entitySets.keys.forEach(dependencies::createOrUpdateProductionViewOfEntitySet)
        }

        override fun after(): Set<Class<out HazelcastInitializationTask<*>>> {
            return setOf(OrganizationsInitializationTask::class.java)
        }

        override fun getName(): String {
            return Task.ENTITY_VIEWS_INITIALIZER.name
        }

        override fun getDependenciesClass(): Class<out Assembler> {
            return Assembler::class.java
        }
    }

    class OrganizationAssembliesInitializerTask : HazelcastInitializationTask<Assembler> {
        override fun getInitialDelay(): Long {
            return 0
        }

        override fun initialize(dependencies: Assembler) {
            dependencies.dbQueryManager.createRenameDatabaseFunctionIfNotExists()
            val currentOrganizations = dependencies.securableObjectTypes.keySet(
                    Predicates.equal("this", SecurableObjectType.Organization)
            ).map { it.first() }.toSet()

            val initializedOrganizations = dependencies.assemblies.keySet(Predicates.equal(INITIALIZED_INDEX, true))

            val organizationsNeedingInitialized: Set<UUID> = currentOrganizations - initializedOrganizations

            organizationsNeedingInitialized.forEach { organizationId ->
                val organizationPrincipal = dependencies.principals[AclKey(organizationId)]
                if (organizationPrincipal == null) {
                    logger.error(
                            "Unable to initialize organization with id {} because principal not found", organizationId
                    )
                } else {
                    logger.info("Initializing database for organization {}", organizationId)
                    val orgDb = dependencies.dbQueryManager.createAndInitializeOrganizationDatabase(organizationId)
                    dependencies.organizationDatabases[organizationId] = orgDb
                }
            }
        }

        override fun after(): Set<Class<out HazelcastInitializationTask<*>>> {
            return setOf(
                    EntitySetViewsInitializerTask::class.java,
                    PostConstructInitializerTask::class.java
            )
        }

        override fun getName(): String {
            return Task.ORGANIZATION_ASSEMBLIES_INITIALIZER.name
        }

        override fun getDependenciesClass(): Class<out Assembler> {
            return Assembler::class.java
        }
    }
}


