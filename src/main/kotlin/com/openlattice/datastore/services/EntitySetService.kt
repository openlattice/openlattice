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
package com.openlattice.datastore.services

import com.codahale.metrics.annotation.Timed
import com.google.common.base.Preconditions.checkState
import com.google.common.base.Preconditions.checkArgument
import com.google.common.collect.Sets
import com.google.common.eventbus.EventBus
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.core.IMap
import com.hazelcast.query.Predicate
import com.hazelcast.query.Predicates
import com.hazelcast.query.QueryConstants
import com.openlattice.assembler.events.MaterializedEntitySetEdmChangeEvent
import com.openlattice.assembler.processors.EntitySetContainsFlagEntryProcessor
import com.openlattice.auditing.AuditRecordEntitySetsManager
import com.openlattice.auditing.AuditingConfiguration
import com.openlattice.auditing.AuditingTypes
import com.openlattice.authorization.*
import com.openlattice.authorization.securable.SecurableObjectType
import com.openlattice.data.storage.partitions.PartitionManager
import com.openlattice.datastore.util.Util
import com.openlattice.edm.EntitySet
import com.openlattice.edm.PostgresEdmManager
import com.openlattice.edm.events.*
import com.openlattice.edm.processors.EntitySetsFlagFilteringAggregator
import com.openlattice.edm.processors.GetEntityTypeFromEntitySetEntryProcessor
import com.openlattice.edm.processors.GetPropertiesFromEntityTypeEntryProcessor
import com.openlattice.edm.requests.MetadataUpdate
import com.openlattice.edm.set.EntitySetFlag
import com.openlattice.edm.set.EntitySetPropertyKey
import com.openlattice.edm.set.EntitySetPropertyMetadata
import com.openlattice.edm.type.AssociationType
import com.openlattice.edm.type.EntityType
import com.openlattice.edm.type.PropertyType
import com.openlattice.edm.types.processors.UpdateEntitySetMetadataProcessor
import com.openlattice.edm.types.processors.UpdateEntitySetPropertyMetadataProcessor
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.hazelcast.processors.AddEntitySetsToLinkingEntitySetProcessor
import com.openlattice.hazelcast.processors.RemoveDataExpirationPolicyProcessor
import com.openlattice.hazelcast.processors.RemoveEntitySetsFromLinkingEntitySetProcessor
import com.openlattice.postgres.mapstores.EntitySetMapstore
import edu.umd.cs.findbugs.classfile.ResourceNotFoundException
import org.slf4j.LoggerFactory
import java.util.*

open class EntitySetService(
        hazelcastInstance: HazelcastInstance,
        private val eventBus: EventBus,
        private val edmManager: PostgresEdmManager,
        private val aclKeyReservations: HazelcastAclKeyReservationService,
        private val authorizations: AuthorizationManager,
        private val partitionManager: PartitionManager,
        private val edm: EdmManager,
        auditingConfiguration: AuditingConfiguration
) : EntitySetManager {

    private val aresManager = AuditRecordEntitySetsManager(
            AuditingTypes(edm, auditingConfiguration),
            this,
            partitionManager,
            authorizations,
            hazelcastInstance
    )


    companion object {
        private val logger = LoggerFactory.getLogger(EntitySetManager::class.java)
    }

    private val entitySets: IMap<UUID, EntitySet> = hazelcastInstance.getMap(HazelcastMap.ENTITY_SETS.name)
    private val entityTypes: IMap<UUID, EntityType> = hazelcastInstance.getMap(HazelcastMap.ENTITY_TYPES.name)
    private val associationTypes: IMap<UUID, AssociationType> =
            hazelcastInstance.getMap(HazelcastMap.ASSOCIATION_TYPES.name)
    private val propertyTypes: IMap<UUID, PropertyType> = hazelcastInstance.getMap(HazelcastMap.PROPERTY_TYPES.name)
    private val entitySetPropertyMetadata: IMap<EntitySetPropertyKey, EntitySetPropertyMetadata> =
            hazelcastInstance.getMap(HazelcastMap.ENTITY_SET_PROPERTY_METADATA.name)

    private val aclKeys: IMap<String, UUID> = hazelcastInstance.getMap(HazelcastMap.ACL_KEYS.name)


    override fun createEntitySet(principal: Principal, entitySet: EntitySet) {
        val entityType = entityTypes.getValue(entitySet.entityTypeId)
        ensureValidEntitySet(entitySet)

        if (entitySet.partitions.isEmpty()) {
            partitionManager.allocatePartitions(entitySet, partitionManager.getPartitionCount())
        }

        if (entityType.category == SecurableObjectType.AssociationType) {
            entitySet.addFlag(EntitySetFlag.ASSOCIATION)
        } else if (entitySet.flags.contains(EntitySetFlag.ASSOCIATION)) {
            entitySet.removeFlag(EntitySetFlag.ASSOCIATION)
        }

        createEntitySet(principal, entitySet, entityType.properties)
    }

    override fun createEntitySet(principal: Principal, entitySet: EntitySet, ownablePropertyTypeIds: Set<UUID>) {
        Principals.ensureUser(principal)
        createEntitySet(entitySet)

        try {
            setupDefaultEntitySetPropertyMetadata(entitySet.id, entitySet.entityTypeId)

            authorizations.setSecurableObjectType(AclKey(entitySet.id), SecurableObjectType.EntitySet)

            authorizations.addPermission(
                    AclKey(entitySet.id),
                    principal,
                    EnumSet.allOf(Permission::class.java)
            )

            ownablePropertyTypeIds
                    .map { propertyTypeId -> AclKey(entitySet.id, propertyTypeId) }
                    .onEach { aclKey ->
                        authorizations.setSecurableObjectType(aclKey, SecurableObjectType.PropertyTypeInEntitySet)
                    }
                    .forEach { aclKey ->
                        authorizations.addPermission(aclKey, principal, EnumSet.allOf(Permission::class.java))
                    }

            val ownablePropertyTypes = propertyTypes.getAll(ownablePropertyTypeIds).values.toList()

            eventBus.post(EntitySetCreatedEvent(entitySet, ownablePropertyTypes))

            if (!entitySet.flags.contains(EntitySetFlag.AUDIT)) {
                aresManager.createAuditEntitySetForEntitySet(entitySet)
            }

        } catch (e: Exception) {
            logger.error("Unable to create entity set $entitySet for principal $principal", e)
            Util.deleteSafely(entitySets, entitySet.id)
            aclKeyReservations.release(entitySet.id)
            throw IllegalStateException("Unable to create entity set: ${entitySet.id}")
        }
    }

    private fun ensureValidEntitySet(entitySet: EntitySet) {
        if (entitySet.isLinking) {
            entitySet.linkedEntitySets.forEach { linkedEntitySetId ->
                checkArgument(getEntityTypeByEntitySetId(linkedEntitySetId).id == entitySet.entityTypeId,
                        "Entity type of linked entity sets must be the same as of the linking entity set")
            }
        }
    }

    private fun setupDefaultEntitySetPropertyMetadata(entitySetId: UUID, entityTypeId: UUID) {
        val et = edm.getEntityType(entityTypeId)
        val propertyTags = et.propertyTags
        et.properties.forEach { propertyTypeId ->
            val key = EntitySetPropertyKey(entitySetId, propertyTypeId)
            val property = edm.getPropertyType(propertyTypeId)
            val metadata = EntitySetPropertyMetadata(
                    property.title,
                    property.description,
                    LinkedHashSet(propertyTags.get(propertyTypeId)),
                    true)
            entitySetPropertyMetadata[key] = metadata
        }
    }

    private fun createEntitySet(entitySet: EntitySet) {
        aclKeyReservations.reserveIdAndValidateType(entitySet)

        checkState(entitySets.putIfAbsent(entitySet.id, entitySet) == null, "Entity set already exists.")
    }

    override fun deleteEntitySet(entitySetId: UUID) {
        val entitySet = Util.getSafely(entitySets, entitySetId)
        val entityType = edm.getEntityType(entitySet.entityTypeId)

        // If this entity set is linked to a linking entity set, we need to collect all the linking ids of the entity
        // set first in order to be able to reindex those, before entity data is unavailable
        if (!entitySet.isLinking) {
            checkAndRemoveEntitySetLinks(entitySetId)
        }

        /*
         * We cleanup permissions first as this will make entity set unavailable, even if delete fails.
         */
        authorizations.deletePermissions(AclKey(entitySetId))
        entityType.properties
                .map { propertyTypeId -> AclKey(entitySetId, propertyTypeId) }
                .forEach { aclKey ->
                    authorizations.deletePermissions(aclKey)
                    entitySetPropertyMetadata.delete(EntitySetPropertyKey(aclKey[0], aclKey[1]))
                }

        Util.deleteSafely(entitySets, entitySetId)
        aclKeyReservations.release(entitySetId)
        eventBus.post(EntitySetDeletedEvent(entitySetId, entityType.id))
        logger.info("Entity set ${entitySet.name}($entitySetId) deleted successfully")
    }

    /**
     * Checks and removes links if deleted entity set is linked to a linking entity set
     *
     * @param entitySetId the id of the deleted entity set
     */
    private fun checkAndRemoveEntitySetLinks(entitySetId: UUID) {
        edmManager.getAllLinkingEntitySetIdsForEntitySet(entitySetId).forEach { linkingEntitySetId ->
            removeLinkedEntitySets(linkingEntitySetId, setOf(entitySetId))
            logger.info(
                    "Removed link between linking entity set ($linkingEntitySetId) and deleted entity set " +
                            "($entitySetId)"
            )
        }
    }

    override fun getEntitySet(entitySetId: UUID): EntitySet? {
        return Util.getSafely(entitySets, entitySetId)
    }

    override fun getEntitySet(entitySetName: String): EntitySet? {
        val id = Util.getSafely(aclKeys, entitySetName)
        return if (id == null) {
            null
        } else {
            getEntitySet(id)
        }
    }

    override fun getEntitySetsAsMap(entitySetIds: Set<UUID>): Map<UUID, EntitySet> {
        return entitySets.getAll(entitySetIds)
    }

    override fun getEntitySets(): Iterable<EntitySet> {
        return edmManager.allEntitySets
    }

    override fun getEntitySetsOfType(entityTypeIds: Set<UUID>): Collection<EntitySet> {
        return entitySets.values(Predicates.`in`(EntitySetMapstore.ENTITY_TYPE_ID_INDEX, *entityTypeIds.toTypedArray()))
    }

    override fun getEntitySetsOfType(entityTypeId: UUID): Collection<EntitySet> {
        return entitySets.values(Predicates.equal(EntitySetMapstore.ENTITY_TYPE_ID_INDEX, entityTypeId))
    }

    override fun getEntitySetIdsOfType(entityTypeId: UUID): Collection<UUID> {
        return entitySets.keySet(Predicates.equal(EntitySetMapstore.ENTITY_TYPE_ID_INDEX, entityTypeId))
    }

    override fun getEntitySetIdsWithFlags(entitySetIds: Set<UUID>, filteringFlags: Set<EntitySetFlag>): Set<UUID> {
        return entitySets.aggregate(
                EntitySetsFlagFilteringAggregator(filteringFlags),
                Predicates.`in`(QueryConstants.KEY_ATTRIBUTE_NAME.value(), *entitySetIds.toTypedArray()) as Predicate<UUID, EntitySet>
        )
    }

    override fun getEntitySetsForOrganization(organizationId: UUID): Set<UUID> {
        return entitySets.keySet(Predicates.equal(EntitySetMapstore.ORGANIZATION_INDEX, organizationId))
    }

    override fun getEntityTypeByEntitySetId(entitySetId: UUID): EntityType {
        val entityTypeId = getEntitySet(entitySetId)!!.entityTypeId
        return edm.getEntityType(entityTypeId)
    }

    override fun getEntityTypeIdsByEntitySetIds(entitySetIds: Set<UUID>): Map<UUID, UUID> {
        return entitySets.getAll(entitySetIds).entries.associateBy(
                { e -> e.key },
                { e -> e.value.entityTypeId }
        )
    }

    override fun getAssociationTypeByEntitySetId(entitySetId: UUID): AssociationType {
        val entityTypeId = getEntitySet(entitySetId)!!.entityTypeId
        return edm.getAssociationType(entityTypeId)
    }

    override fun getAssociationTypeDetailsByEntitySetIds(entitySetIds: Set<UUID>): Map<UUID, AssociationType> {
        val entityTypeIdsByEntitySetId = getEntityTypeIdsByEntitySetIds(entitySetIds)

        val associationTypesByEntityTypeId = associationTypes.getAll(
                Sets.newHashSet(entityTypeIdsByEntitySetId.values)
        )

        return entitySetIds.associateWith {
            associationTypesByEntityTypeId.getValue(entityTypeIdsByEntitySetId.getValue(it))
        }
    }

    override fun isAssociationEntitySet(entitySetId: UUID): Boolean {
        return containsFlag(entitySetId, EntitySetFlag.ASSOCIATION)
    }

    override fun containsFlag(entitySetId: UUID, flag: EntitySetFlag): Boolean {
        return entitySets.executeOnKey(entitySetId, EntitySetContainsFlagEntryProcessor(flag)) as Boolean
    }

    override fun entitySetsContainFlag(entitySetIds: Set<UUID>, flag: EntitySetFlag): Boolean {
        return entitySets.executeOnKeys(entitySetIds, EntitySetContainsFlagEntryProcessor(flag)).values.any { it as Boolean }
    }

    @Timed
    override fun getPropertyTypesForEntitySet(entitySetId: UUID): Map<UUID, PropertyType> {
        val maybeEtId = entitySets.executeOnKey(entitySetId, GetEntityTypeFromEntitySetEntryProcessor())
                as? UUID
                ?: throw  ResourceNotFoundException("Entity set $entitySetId does not exist.")

        val maybeEtProps = entityTypes.executeOnKey(maybeEtId, GetPropertiesFromEntityTypeEntryProcessor())
                as? Set<UUID>
                ?: throw  ResourceNotFoundException("Entity type $maybeEtId does not exist.")

        return propertyTypes.getAll(maybeEtProps)
    }

    override fun getEntitySetPropertyMetadata(entitySetId: UUID, propertyTypeId: UUID): EntitySetPropertyMetadata {
        val key = EntitySetPropertyKey(entitySetId, propertyTypeId)
        if (!entitySetPropertyMetadata.containsKey(key)) {
            val entityTypeId = getEntitySet(entitySetId)!!.entityTypeId
            setupDefaultEntitySetPropertyMetadata(entitySetId, entityTypeId)
        }
        return entitySetPropertyMetadata.getValue(key)
    }

    override fun getAllEntitySetPropertyMetadata(entitySetId: UUID): Map<UUID, EntitySetPropertyMetadata> {
        return getEntityTypeByEntitySetId(entitySetId).properties.associateWith {
            getEntitySetPropertyMetadata(entitySetId, it)
        }
    }

    override fun getAllEntitySetPropertyMetadataForIds(
            entitySetIds: Set<UUID>
    ): Map<UUID, Map<UUID, EntitySetPropertyMetadata>> {
        val entitySetsById = entitySets.getAll(entitySetIds)
        val entityTypesById = entityTypes.getAll(entitySetsById.values.map(EntitySet::getEntityTypeId).toSet())

        val keys = entitySetIds
                .flatMap { entitySetId ->
                    entityTypesById.getValue(entitySetsById.getValue(entitySetId).entityTypeId).properties
                            .map { propertyTypeId ->
                                EntitySetPropertyKey(entitySetId, propertyTypeId)
                            }
                }
                .toSet()

        val metadataMap = entitySetPropertyMetadata.getAll(keys)

        val missingKeys = Sets.difference(keys, metadataMap.keys).toSet()
        val missingPropertyTypesById = propertyTypes.getAll(
                missingKeys.map(EntitySetPropertyKey::getPropertyTypeId).toSet()
        )

        missingKeys.forEach { newKey ->
            val propertyType = missingPropertyTypesById.getValue(newKey.propertyTypeId)
            val propertyTags = entityTypesById.getValue(entitySetsById.getValue(newKey.entitySetId).entityTypeId)
                    .propertyTags.get(newKey.propertyTypeId)

            val defaultMetadata = EntitySetPropertyMetadata(
                    propertyType.title,
                    propertyType.description,
                    LinkedHashSet(propertyTags),
                    true
            )
            metadataMap[newKey] = defaultMetadata
            entitySetPropertyMetadata[newKey] = defaultMetadata
        }

        return metadataMap.entries
                .groupBy { it.key.entitySetId }
                .mapValues { it.value.associateBy({ it.key.propertyTypeId }, { it.value }) }
    }

    override fun updateEntitySetPropertyMetadata(entitySetId: UUID, propertyTypeId: UUID, update: MetadataUpdate) {
        val key = EntitySetPropertyKey(entitySetId, propertyTypeId)
        entitySetPropertyMetadata.executeOnKey(key, UpdateEntitySetPropertyMetadataProcessor(update))
    }

    override fun updateEntitySetMetadata(entitySetId: UUID, update: MetadataUpdate) {
        if (update.name.isPresent || update.organizationId.isPresent) {
            val oldEntitySet = getEntitySet(entitySetId)!!

            if (update.name.isPresent) {
                aclKeyReservations.renameReservation(entitySetId, update.name.get())

                // If entity set name is changed, change also name of materialized view
                eventBus.post(EntitySetNameUpdatedEvent(entitySetId, update.name.get(), oldEntitySet.name))
            }

            if (update.organizationId.isPresent) {
                // If an entity set is being moved across organizations, its audit entity sets should also be moved to
                // the new organization
                val aclKey = AclKey(entitySetId)

                val auditEntitySetIds = Sets.union(
                        aresManager.getAuditRecordEntitySets(aclKey), aresManager.getAuditEdgeEntitySets(aclKey)
                )

                entitySets.executeOnKeys(
                        auditEntitySetIds,
                        UpdateEntitySetMetadataProcessor(
                                MetadataUpdate(
                                        Optional.empty(),
                                        Optional.empty(),
                                        Optional.empty(),
                                        Optional.empty(),
                                        Optional.empty(),
                                        Optional.empty(),
                                        Optional.empty(),
                                        Optional.empty(),
                                        Optional.empty(),
                                        update.organizationId,
                                        Optional.empty(),
                                        Optional.empty()
                                )
                        )
                )

                // If an entity set is being moved across organizations, its materialized entity set should be deleted
                // from old organization assembly
                eventBus.post(EntitySetOrganizationUpdatedEvent(entitySetId, oldEntitySet.organizationId))
            }
        }

        val newEntitySet = entitySets.executeOnKey(entitySetId, UpdateEntitySetMetadataProcessor(update)) as EntitySet
        eventBus.post(EntitySetMetadataUpdatedEvent(newEntitySet))
    }

    override fun addLinkedEntitySets(entitySetId: UUID, linkedEntitySets: Set<UUID>): Int {
        val linkingEntitySet = Util.getSafely(entitySets, entitySetId)
        val startSize = linkingEntitySet.linkedEntitySets.size
        val updatedLinkingEntitySet = entitySets.executeOnKey(
                entitySetId,
                AddEntitySetsToLinkingEntitySetProcessor(linkedEntitySets)
        ) as EntitySet

        markMaterializedEntitySetDirtyWithEdmChanges(linkingEntitySet.id)
        eventBus.post(LinkedEntitySetAddedEvent(entitySetId))

        return updatedLinkingEntitySet.linkedEntitySets.size - startSize
    }

    override fun removeLinkedEntitySets(entitySetId: UUID, linkedEntitySets: Set<UUID>): Int {
        val linkingEntitySet = Util.getSafely(entitySets, entitySetId)
        val startSize = linkingEntitySet.linkedEntitySets.size
        val updatedLinkingEntitySet = entitySets.executeOnKey(
                entitySetId,
                RemoveEntitySetsFromLinkingEntitySetProcessor(linkedEntitySets)
        ) as EntitySet

        markMaterializedEntitySetDirtyWithEdmChanges(linkingEntitySet.id)
        eventBus.post(LinkedEntitySetRemovedEvent(entitySetId))

        return startSize - updatedLinkingEntitySet.linkedEntitySets.size
    }

    private fun markMaterializedEntitySetDirtyWithEdmChanges(entitySetId: UUID) {
        eventBus.post(MaterializedEntitySetEdmChangeEvent(entitySetId))
    }

    override fun getLinkedEntitySets(entitySetId: UUID): Set<EntitySet> {
        val linkedEntitySetIds = getEntitySet(entitySetId)!!.linkedEntitySets ?: setOf()
        return entitySets.getAll(linkedEntitySetIds).values.toSet()
    }

    override fun getLinkedEntitySetIds(entitySetId: UUID): Set<UUID> {
        return getEntitySet(entitySetId)!!.linkedEntitySets ?: setOf()
    }

    override fun removeDataExpirationPolicy(entitySetId: UUID) {
        entitySets.executeOnKey(entitySetId, RemoveDataExpirationPolicyProcessor())
    }

    override fun getAuditRecordEntitySetsManager(): AuditRecordEntitySetsManager {
        return aresManager
    }
}