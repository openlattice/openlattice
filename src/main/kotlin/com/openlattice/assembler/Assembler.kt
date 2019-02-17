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
import com.hazelcast.query.Predicates
import com.openlattice.assembler.PostgresRoles.Companion.buildOrganizationUserId
import com.openlattice.assembler.processors.InitializeOrganizationAssemblyProcessor
import com.openlattice.assembler.processors.MaterializeEntitySetsProcessor
import com.openlattice.authorization.AclKey
import com.openlattice.authorization.AuthorizationManager
import com.openlattice.authorization.DbCredentialService
import com.openlattice.authorization.SecurablePrincipal
import com.openlattice.authorization.securable.SecurableObjectType
import com.openlattice.data.storage.MetadataOption
import com.openlattice.data.storage.selectEntitySetWithCurrentVersionOfPropertyTypes
import com.openlattice.datastore.exceptions.ResourceNotFoundException
import com.openlattice.edm.EntitySet
import com.openlattice.edm.events.EntitySetCreatedEvent
import com.openlattice.edm.events.PropertyTypesAddedToEntitySetEvent
import com.openlattice.edm.type.EntityType
import com.openlattice.edm.type.PropertyType
import com.openlattice.hazelcast.HazelcastMap.*
import com.openlattice.organization.Organization
import com.openlattice.organization.OrganizationEntitySetFlag
import com.openlattice.organization.OrganizationIntegrationAccount
import com.openlattice.postgres.DataTables
import com.openlattice.postgres.mapstores.OrganizationAssemblyMapstore.INITIALIZED_INDEX
import com.zaxxer.hikari.HikariDataSource
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind
import org.slf4j.LoggerFactory
import java.util.*

const val SCHEMA = "openlattice"
const val PRODUCTION = "olprod"
private val logger = LoggerFactory.getLogger(Assembler::class.java)

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class Assembler(
        private val assemblerConfiguration: AssemblerConfiguration,
        val authz: AuthorizationManager,
        private val dbCredentialService: DbCredentialService,
        val hds: HikariDataSource,
        metricRegistry: MetricRegistry,
        hazelcast: HazelcastInstance,
        eventBus: EventBus

) {
    private val entitySets = hazelcast.getMap<UUID, EntitySet>(ENTITY_SETS.name)
    private val entityTypes = hazelcast.getMap<UUID, EntityType>(ENTITY_TYPES.name)
    private val propertyTypes = hazelcast.getMap<UUID, PropertyType>(PROPERTY_TYPES.name)
    private val assemblies = hazelcast.getMap<UUID, OrganizationAssembly>(ASSEMBLIES.name)
    private val securableObjectTypes = hazelcast.getMap<AclKey, SecurableObjectType>(SECURABLE_OBJECT_TYPES.name)
    private val principals = hazelcast.getMap<AclKey, SecurablePrincipal>(PRINCIPALS.name)
    private val createOrganizationTimer = metricRegistry.timer(name(Assembler::class.java, "createOrganization"))

    init {
        eventBus.register(this)
    }

    fun getMaterializedEntitySets(organizationId: UUID): Set<UUID> {
        return assemblies[organizationId]?.entitySetIds ?: setOf()
    }

    fun getOrganizationAssembly(organizationId: UUID): OrganizationAssembly {
        return assemblies[organizationId]!!
    }

    @Subscribe
    fun handleEntitySetCreated(entitySetCreatedEvent: EntitySetCreatedEvent) {
        createOrUpdateProductionViewOfEntitySet(entitySetCreatedEvent.entitySet.id)
    }

    @Subscribe
    fun handlePropertyTypeAddedToEntitySet(propertyTypesAddedToEntitySetEvent: PropertyTypesAddedToEntitySetEvent) {
        createOrUpdateProductionViewOfEntitySet(propertyTypesAddedToEntitySetEvent.entitySetId)
    }

    fun createOrganization(organization: Organization) {
        createOrganization(organization.id, organization.principal.id)
    }

    fun createOrganization(organizationId: UUID, organizationPrincipalId: String) {
        createOrganizationTimer.time().use {
            assemblies.set(organizationId, OrganizationAssembly(organizationId, organizationPrincipalId))
            assemblies.executeOnKey(organizationId, InitializeOrganizationAssemblyProcessor())
            return@use
        }
    }

    fun initialize() {
        initializeEntitySetViews()
        initializeOrganizations()
    }

    private fun initializeEntitySetViews() {
        entitySets.keys.forEach(this::createOrUpdateProductionViewOfEntitySet)
    }

    private fun initializeOrganizations() {
        val currentOrganizations =
                securableObjectTypes.keySet(
                        Predicates.equal("this", SecurableObjectType.Organization)
                ).map { it.first() }.toSet()

        val initializedOrganizations = assemblies.keySet(Predicates.equal(INITIALIZED_INDEX, true))

        val organizationsNeedingInitialized: Set<UUID> = currentOrganizations - initializedOrganizations

        organizationsNeedingInitialized.forEach { organizationId ->
            val organizationPrincipal = principals[AclKey(organizationId)]
            if (organizationPrincipal == null) {
                logger.error("Unable to initialize organization with id {} because principal not found", organizationId)
            } else {
                logger.info("Initializing database for organization {}", organizationId)
                createOrganization(organizationId, organizationPrincipal.principal.id)
            }
        }
    }

    fun materializeEntitySets(
            organizationId: UUID,
            authorizedPropertyTypesByEntitySet: Map<UUID, Map<UUID, PropertyType>>
    ): Map<UUID, Set<OrganizationEntitySetFlag>> {
        assemblies.executeOnKey(organizationId, MaterializeEntitySetsProcessor(authorizedPropertyTypesByEntitySet))
        return getMaterializedEntitySets(organizationId).map {
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
                entitySet.isLinking
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
}


