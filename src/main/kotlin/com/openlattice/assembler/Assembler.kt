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
import com.openlattice.assembler.PostgresRoles.Companion.buildOrganizationUserId
import com.openlattice.assembler.processors.*
import com.openlattice.assembler.tasks.UsersAndRolesInitializationTask
import com.openlattice.authorization.*
import com.openlattice.authorization.securable.SecurableObjectType
import com.openlattice.controllers.exceptions.ResourceNotFoundException
import com.openlattice.data.storage.MetadataOption
import com.openlattice.data.storage.selectEntitySetWithCurrentVersionOfPropertyTypes
import com.openlattice.datastore.util.Util
import com.openlattice.edm.EntitySet
import com.openlattice.edm.events.EntitySetCreatedEvent
import com.openlattice.edm.type.EntityType
import com.openlattice.edm.type.PropertyType
import com.openlattice.hazelcast.HazelcastMap.*
import com.openlattice.hazelcast.processors.AddFlagsToMaterializedEntitySetProcessor
import com.openlattice.hazelcast.serializers.AssemblerConnectionManagerDependent
import com.openlattice.organization.Organization
import com.openlattice.organization.OrganizationEntitySetFlag
import com.openlattice.organization.OrganizationIntegrationAccount
import com.openlattice.organizations.events.MembersAddedToOrganizationEvent
import com.openlattice.organizations.events.MembersRemovedFromOrganizationEvent
import com.openlattice.organizations.tasks.OrganizationsInitializationTask
import com.openlattice.postgres.DataTables
import com.openlattice.postgres.mapstores.MaterializedEntitySetMapStore
import com.openlattice.postgres.mapstores.OrganizationAssemblyMapstore.*
import com.openlattice.tasks.HazelcastInitializationTask
import com.openlattice.tasks.HazelcastTaskDependencies
import com.openlattice.tasks.PostConstructInitializerTaskDependencies.PostConstructInitializerTask
import com.openlattice.tasks.Task
import com.zaxxer.hikari.HikariDataSource
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind
import org.slf4j.LoggerFactory
import java.util.*

const val SCHEMA = "openlattice"

private val logger = LoggerFactory.getLogger(Assembler::class.java)

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class Assembler(
        val authz: AuthorizationManager,
        private val dbCredentialService: DbCredentialService,
        val hds: HikariDataSource,
        metricRegistry: MetricRegistry,
        hazelcast: HazelcastInstance,
        eventBus: EventBus

) : HazelcastTaskDependencies, AssemblerConnectionManagerDependent {

    private val entitySets = hazelcast.getMap<UUID, EntitySet>(ENTITY_SETS.name)
    private val entityTypes = hazelcast.getMap<UUID, EntityType>(ENTITY_TYPES.name)
    private val propertyTypes = hazelcast.getMap<UUID, PropertyType>(PROPERTY_TYPES.name)
    private val assemblies = hazelcast.getMap<UUID, OrganizationAssembly>(ASSEMBLIES.name)
    private val materializedEntitySets = hazelcast.getMap<EntitySetAssemblyKey, MaterializedEntitySet>(MATERIALIZED_ENTITY_SETS.name)
    private val securableObjectTypes = hazelcast.getMap<AclKey, SecurableObjectType>(SECURABLE_OBJECT_TYPES.name)
    private val principals = hazelcast.getMap<AclKey, SecurablePrincipal>(PRINCIPALS.name)
    private val createOrganizationTimer = metricRegistry.timer(name(Assembler::class.java, "createOrganization"))
    private val deleteOrganizationTimer = metricRegistry.timer(name(Assembler::class.java, "deleteOrganization"))
    private lateinit var acm: AssemblerConnectionManager

    init {
        eventBus.register(this)
    }

    override fun init(assemblerConnectionManager: AssemblerConnectionManager) {
        this.acm = assemblerConnectionManager
    }

    fun getMaterializedEntitySetsInOrganization(organizationId: UUID): Set<UUID> {
        return assemblies[organizationId]?.materializedEntitySets?.keys ?: setOf()
    }

    /**
     * Returns true, if the entity set is materialized in any of the organization assemblies
     */
    private fun isEntitySetMaterialized(entitySetId: UUID): Boolean {
        return materializedEntitySets
                .keySet(entitySetIdPredicate(entitySetId))
                .isNotEmpty()
    }

    fun flagMaterializedEntitySet(entitySetId: UUID, flag: OrganizationEntitySetFlag) {
        if (isEntitySetMaterialized(entitySetId)) {
            materializedEntitySets.executeOnEntries(
                    AddFlagsToMaterializedEntitySetProcessor(setOf(flag)),
                    entitySetIdPredicate(entitySetId))
        }
    }


    @Subscribe
    fun handleEntitySetCreated(entitySetCreatedEvent: EntitySetCreatedEvent) {
        createOrUpdateProductionViewOfEntitySet(entitySetCreatedEvent.entitySet.id)
        assemblies.executeOnKey(
                entitySetCreatedEvent.entitySet.organizationId,
                CreateProductionForeignTableOfEntitySetProcessor(entitySetCreatedEvent.entitySet.id).init(acm))
    }

    fun createOrganization(organization: Organization) {
        createOrganization(organization.id, organization.principal.id)
    }

    fun createOrganization(organizationId: UUID, dbname: String) {
        createOrganizationTimer.time().use {
            assemblies.set(organizationId, OrganizationAssembly(organizationId, dbname))
            assemblies.executeOnKey(organizationId, InitializeOrganizationAssemblyProcessor().init(acm))
            return@use
        }
    }

    fun destroyOrganization(organizationId: UUID) {
        deleteOrganizationTimer.time().use {
            assemblies.executeOnKey(organizationId, DeleteOrganizationAssemblyProcessor().init(acm))
            Util.deleteSafely(assemblies, organizationId)
        }
    }

    @Subscribe
    fun handleAddMembersToOrganization(event: MembersAddedToOrganizationEvent) {
        assemblies.executeOnKey(
                event.organizationId,
                AddMembersToOrganizationAssemblyProcessor(event.newMembers).init(acm))
    }

    @Subscribe
    fun removeMembersFromOrganization(event: MembersRemovedFromOrganizationEvent) {
        assemblies.executeOnKey(event.organizationId,
                RemoveMembersFromOrganizationAssemblyProcessor(event.members).init(acm))
    }

    fun materializeEntitySets(
            organizationId: UUID,
            authorizedPropertyTypesByEntitySet: Map<UUID, Map<UUID, PropertyType>>
    ): Map<UUID, Set<OrganizationEntitySetFlag>> {
        // check if organization is initialized
        if (!assemblies.containsKey(organizationId) || !assemblies[organizationId]!!.initialized) {
            throw IllegalStateException("Organization assembly is not initialized for organization $organizationId")
        }

        authorizedPropertyTypesByEntitySet.forEach { entitySetId, authorizedPropertyTypes ->
            // even if we re-materialize, we would clear all flags
            materializedEntitySets.set(
                    EntitySetAssemblyKey(entitySetId, organizationId),
                    MaterializedEntitySet(EntitySetAssemblyKey(entitySetId, organizationId)))
            materializedEntitySets.executeOnKey(
                    EntitySetAssemblyKey(entitySetId, organizationId),
                    MaterializeEntitySetProcessor(authorizedPropertyTypes).init(acm)
            )
        }

        return getMaterializedEntitySetsInOrganization(organizationId).map {
            it to (setOf(OrganizationEntitySetFlag.MATERIALIZED) + getInternalEntitySetFlag(organizationId, it))
        }.toMap()
    }

    private fun createOrUpdateProductionViewOfEntitySet(entitySetId: UUID) {
        val entitySet = entitySets.getValue(entitySetId)
        val authorizedPropertyTypes = propertyTypes
                .getAll(entityTypes.getValue(entitySets.getValue(entitySetId).entityTypeId).properties)
        val propertyFqns = authorizedPropertyTypes.mapValues {
            DataTables.quote(it.value.type.fullQualifiedNameAsString)
        }

        val sql = selectEntitySetWithCurrentVersionOfPropertyTypes(
                mapOf(entitySetId to Optional.empty()),
                propertyFqns,
                authorizedPropertyTypes.values.map(PropertyType::getId),
                mapOf(entitySetId to authorizedPropertyTypes.keys),
                mapOf(),
                EnumSet.allOf(MetadataOption::class.java),
                authorizedPropertyTypes.mapValues { it.value.datatype == EdmPrimitiveTypeKind.Binary },
                entitySet.isLinking,
                false //Always provide entity set id
        )

        //Drop and recreate the view with the latest schema
        hds.connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute("DROP VIEW IF EXISTS $PRODUCTION_VIEWS_SCHEMA.\"$entitySetId\"")
                stmt.execute("CREATE OR REPLACE VIEW $PRODUCTION_VIEWS_SCHEMA.\"$entitySetId\" AS $sql")
                return@use
            }
        }
    }

    fun getOrganizationIntegrationAccount(organizationId: UUID): OrganizationIntegrationAccount {
        val organizationUserId = buildOrganizationUserId(organizationId)
        val credential = this.dbCredentialService.getDbCredential(organizationUserId)
                ?: throw ResourceNotFoundException("Organization credential not found.")
        return OrganizationIntegrationAccount(organizationUserId, credential)
    }

    private fun getInternalEntitySetFlag(organizationId: UUID, entitySetId: UUID): Set<OrganizationEntitySetFlag> {
        return if (entitySets[entitySetId]?.organizationId == organizationId) {
            setOf(OrganizationEntitySetFlag.INTERNAL)
        } else {
            setOf()
        }
    }

    private fun entitySetIdPredicate(entitySetId: UUID): Predicate<*, *> {
        return Predicates.equal(MaterializedEntitySetMapStore.ENTITY_SET_ID_INDEX, entitySetId)
    }

    /**
     * This class is responsible for refreshing all entity set views at startup.
     */
    class EntitySetViewsInitializerTask : HazelcastInitializationTask<Assembler> {
        override fun getInitialDelay(): Long {
            return 0
        }

        override fun initialize(dependencies: Assembler) {
            dependencies.entitySets.keys.forEach(dependencies::createOrUpdateProductionViewOfEntitySet)
        }

        override fun after(): Set<Class<out HazelcastInitializationTask<*>>> {
            return setOf(
                    OrganizationsInitializationTask::class.java,
                    UsersAndRolesInitializationTask::class.java
            )
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
            val currentOrganizations =
                    dependencies
                            .securableObjectTypes.keySet(
                            Predicates.equal(
                                    "this"
                                    , SecurableObjectType.Organization
                            )
                    )
                            .map { it.first() }
                            .toSet()

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
                    dependencies.createOrganization(organizationId, PostgresDatabases.buildOrganizationDatabaseName(organizationId))
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


