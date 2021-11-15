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
import com.google.common.base.Preconditions.checkArgument
import com.google.common.base.Preconditions.checkState
import com.google.common.collect.Lists
import com.google.common.collect.Sets
import com.google.common.eventbus.EventBus
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.query.Predicates
import com.hazelcast.query.QueryConstants
import com.openlattice.assembler.events.MaterializedEntitySetEdmChangeEvent
import com.openlattice.assembler.processors.EntitySetContainsFlagEntryProcessor
import com.openlattice.auditing.AuditRecordEntitySetsManager
import com.openlattice.auditing.AuditingConfiguration
import com.openlattice.auditing.AuditingTypes
import com.openlattice.authorization.AclKey
import com.openlattice.authorization.AuthorizationManager
import com.openlattice.authorization.HazelcastAclKeyReservationService
import com.openlattice.authorization.Permission
import com.openlattice.authorization.Principal
import com.openlattice.authorization.Principals
import com.openlattice.authorization.securable.SecurableObjectType
import com.openlattice.authorization.securable.SecurableObjectType.PropertyTypeInEntitySet
import com.openlattice.controllers.exceptions.ResourceNotFoundException
import com.openlattice.data.storage.PostgresEntitySetSizesInitializationTask
import com.openlattice.datasets.DataSetService
import com.openlattice.datasets.SecurableObjectMetadata
import com.openlattice.datasets.SecurableObjectMetadataUpdate
import com.openlattice.datastore.util.Util
import com.openlattice.edm.EntitySet
import com.openlattice.edm.events.*
import com.openlattice.edm.processors.EntitySetsFlagFilteringAggregator
import com.openlattice.edm.processors.GetEntityTypeFromEntitySetEntryProcessor
import com.openlattice.edm.processors.GetNormalEntitySetIdsEntryProcessor
import com.openlattice.edm.processors.GetPropertiesFromEntityTypeEntryProcessor
import com.openlattice.edm.requests.MetadataUpdate
import com.openlattice.edm.set.EntitySetFlag
import com.openlattice.edm.set.EntitySetPropertyMetadata
import com.openlattice.edm.type.AssociationType
import com.openlattice.edm.type.EntityType
import com.openlattice.edm.type.PropertyType
import com.openlattice.edm.types.processors.UpdateEntitySetMetadataProcessor
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.hazelcast.processors.AddEntitySetsToLinkingEntitySetProcessor
import com.openlattice.hazelcast.processors.RemoveDataExpirationPolicyProcessor
import com.openlattice.hazelcast.processors.RemoveEntitySetsFromLinkingEntitySetProcessor
import com.openlattice.postgres.PostgresColumn
import com.openlattice.postgres.PostgresColumn.ACL_KEY
import com.openlattice.postgres.PostgresColumn.ENTITY_TYPE_ID
import com.openlattice.postgres.PostgresColumn.ID
import com.openlattice.postgres.PostgresColumn.PRINCIPAL_ID
import com.openlattice.postgres.PostgresTable.ENTITY_SETS
import com.openlattice.postgres.PostgresTable.PERMISSIONS
import com.openlattice.postgres.ResultSetAdapters
import com.openlattice.postgres.mapstores.EntitySetMapstore
import com.openlattice.postgres.streams.BasePostgresIterable
import com.openlattice.postgres.streams.StatementHolderSupplier
import com.openlattice.rhizome.DelegatedIntSet
import com.openlattice.rhizome.hazelcast.DelegatedUUIDSet
import com.openlattice.search.requests.EntityNeighborsFilter
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.util.*
import kotlin.collections.LinkedHashSet

@Service
class EntitySetService(
    hazelcastInstance: HazelcastInstance,
    private val eventBus: EventBus,
    private val aclKeyReservations: HazelcastAclKeyReservationService,
    private val authorizations: AuthorizationManager,
    private val edm: EdmManager,
    private val hds: HikariDataSource,
    private val dataSetService: DataSetService,
    auditingConfiguration: AuditingConfiguration
) : EntitySetManager {

    private val aresManager = AuditRecordEntitySetsManager(
            AuditingTypes(edm, auditingConfiguration),
            this,
            authorizations,
            hazelcastInstance
    )

    companion object {
        private val logger = LoggerFactory.getLogger(EntitySetManager::class.java)
    }

    private val entitySets = HazelcastMap.ENTITY_SETS.getMap(hazelcastInstance)
    private val entityTypes = HazelcastMap.ENTITY_TYPES.getMap(hazelcastInstance)
    private val associationTypes = HazelcastMap.ASSOCIATION_TYPES.getMap(hazelcastInstance)
    private val propertyTypes = HazelcastMap.PROPERTY_TYPES.getMap(hazelcastInstance)
    private val deletedEntitySets = HazelcastMap.DELETED_ENTITY_SETS.getMap(hazelcastInstance)
    private val objectMetadata = HazelcastMap.OBJECT_METADATA.getMap(hazelcastInstance)

    private val aclKeys = HazelcastMap.ACL_KEYS.getMap(hazelcastInstance)

    @Timed
    override fun createEntitySet(principal: Principal, entitySet: EntitySet): UUID {
        ensureValidEntitySet(entitySet)
        when {
            entitySet.isMetadataEntitySet -> {
                Principals.ensureRole(principal)
            }
            entitySet.isAudit -> {
                Principals.ensureUserOrRole(principal)
            }
            else -> {
                Principals.ensureUser(principal)
            }
        }

        val entityType = entityTypes.getValue(entitySet.entityTypeId)

        if (entityType.category == SecurableObjectType.AssociationType) {
            entitySet.addFlag(EntitySetFlag.ASSOCIATION)
        } else if (entitySet.flags.contains(EntitySetFlag.ASSOCIATION)) {
            entitySet.removeFlag(EntitySetFlag.ASSOCIATION)
        }

        val entitySetId = reserveEntitySetIfNotExists(entitySet)

        try {
            setupDefaultEntitySetPropertyMetadata(entitySetId, entitySet.entityTypeId)

            val aclKey = AclKey(entitySetId)

            authorizations.setSecurableObjectType(aclKey, SecurableObjectType.EntitySet)
            authorizations.addPermission(aclKey, principal, EnumSet.allOf(Permission::class.java))

            val aclKeys = entityType.properties.mapTo(mutableSetOf()) { propertyTypeId ->
                AclKey(
                        entitySetId,
                        propertyTypeId
                )
            }

            authorizations.setSecurableObjectTypes(aclKeys, PropertyTypeInEntitySet)
            authorizations.addPermissions(
                    aclKeys,
                    principal,
                    EnumSet.allOf(Permission::class.java),
                    PropertyTypeInEntitySet
            )

            if (!entitySet.isMetadataEntitySet) {
                aresManager.createAuditEntitySetForEntitySet(entitySet)
            }

            val ownablePropertyTypes = propertyTypes.getAll(entityType.properties).values.toList()
            eventBus.post(EntitySetCreatedEvent(entitySet, ownablePropertyTypes))
            dataSetService.indexDataSet(entitySet.id)

        } catch (e: Exception) {
            logger.error("Unable to create entity set ${entitySet.name} (${entitySet.id}) for principal $principal", e)
            deleteEntitySet(entitySet, entityType)
            throw IllegalStateException("Unable to create entity set ${entitySet.id}. $e")
        }

        return entitySetId
    }

    @Timed
    override fun getTransportedEntitySetsOfType(entityTypeId: UUID): Set<EntitySet> {
        return entitySets.values(
                Predicates.and(
                        Predicates.equal<UUID, EntitySet>(EntitySetMapstore.FLAGS_INDEX, EntitySetFlag.TRANSPORTED),
                        Predicates.notEqual<UUID, EntitySet>(EntitySetMapstore.FLAGS_INDEX, EntitySetFlag.LINKING),
                        Predicates.equal<UUID, EntitySet>(EntitySetMapstore.ENTITY_TYPE_ID_INDEX, entityTypeId)
                )
        ).toSet()
    }

    @Timed
    override fun getAuthorizedNeighborEntitySets(
            principals: Set<Principal>,
            entitySetIds: Set<UUID>,
            filter: EntityNeighborsFilter
    ): EntityNeighborsFilter {
        val entityTypeIds = getEntityTypeIdsByEntitySetIds(entitySetIds).values.toSet()

        val (srcEntityTypeIds, assocEntityTypeIds, dstEntityTypeIds) = edm.getSrcAssocDstInvolvingEntityTypes(entityTypeIds)

        if (srcEntityTypeIds.isEmpty() || assocEntityTypeIds.isEmpty() || dstEntityTypeIds.isEmpty()) {
            return EntityNeighborsFilter(filter.entityKeyIds, Optional.of(setOf()), Optional.of(setOf()), Optional.of(setOf()))
        }

        val sql = getAuthorizedNeighborEntitySetsQuery(principals, srcEntityTypeIds, assocEntityTypeIds, dstEntityTypeIds, filter)
        val entitySetIdsByEntityTypeIds = BasePostgresIterable(StatementHolderSupplier(hds, sql)) {
            ResultSetAdapters.id(it) to ResultSetAdapters.entityTypeId(it)
        }.groupBy { (_, etid) -> etid }.mapValues { entry -> entry.value.map { (esid, _) -> esid } }

        val srcEntitySetIds = getFilteredEntitySetOfTypeGroup(entitySetIdsByEntityTypeIds, srcEntityTypeIds, filter.srcEntitySetIds)
        val dstEntitySetIds = getFilteredEntitySetOfTypeGroup(entitySetIdsByEntityTypeIds, dstEntityTypeIds, filter.dstEntitySetIds)
        val assocEntitySetIds = getFilteredEntitySetOfTypeGroup(entitySetIdsByEntityTypeIds, assocEntityTypeIds, filter.associationEntitySetIds)

        return EntityNeighborsFilter(
                filter.entityKeyIds,
                Optional.of(srcEntitySetIds),
                Optional.of(dstEntitySetIds),
                Optional.of(assocEntitySetIds)
        )
    }

    private fun getFilteredEntitySetOfTypeGroup(entitySetIdsByEntityTypeIds: Map<UUID, List<UUID>>, entityTypeIds: Set<UUID>, maybeEntitySetIds: Optional<Set<UUID>>): Set<UUID> {
        val unfilteredEntitySetIds = entityTypeIds.flatMap { entitySetIdsByEntityTypeIds[it] ?: listOf() }

        if (maybeEntitySetIds.isPresent) {
            val entitySetIdFilter = maybeEntitySetIds.get()
            return unfilteredEntitySetIds.filter { entitySetIdFilter.contains(it) }.toSet()
        }

        return unfilteredEntitySetIds.toSet()

    }

    private fun getAuthorizedNeighborEntitySetsQuery(
            principals: Set<Principal>,
            srcEntityTypeIds: Set<UUID>,
            assocEntityTypeIds: Set<UUID>,
            dstEntityTypeIds: Set<UUID>,
            filter: EntityNeighborsFilter
    ): String {

        val subFilters = Lists.newArrayListWithCapacity<String>(3)
        val entityTypeIdsWithoutSetIdRestrictions = mutableSetOf<UUID>()

        listOf(
                filter.srcEntitySetIds to srcEntityTypeIds,
                filter.associationEntitySetIds to assocEntityTypeIds,
                filter.dstEntitySetIds to dstEntityTypeIds
        ).forEach { (maybeEntitySetIds, entityTypeIds) ->
            maybeEntitySetIds.ifPresentOrElse({
                if (it.isNotEmpty()) {
                    subFilters.add(whereEntityTypeIdsAndEntitySetIds(entityTypeIds, it))
                }
            }, {
                entityTypeIdsWithoutSetIdRestrictions.addAll(entityTypeIds)
            })
        }

        if (entityTypeIdsWithoutSetIdRestrictions.isNotEmpty()) {
            subFilters.add(whereEntityTypeIdsAndEntitySetIds(entityTypeIdsWithoutSetIdRestrictions))
        }

        val subClauses = subFilters.joinToString(" OR ") { "( $it )" }

        val authFilter = "${PERMISSIONS.name}.${PRINCIPAL_ID.name} = ANY('{${principals.joinToString { "\"${it.id}\"" }}}') "

        return """
            SELECT ${ID.name}, ${ENTITY_TYPE_ID.name}
            FROM ${ENTITY_SETS.name}
            INNER JOIN ${PERMISSIONS.name}
            ON ${PERMISSIONS.name}.${ACL_KEY.name} = ARRAY[ ${ENTITY_SETS.name}.${ID.name} ]
            WHERE ( $subClauses )
            AND $authFilter
        """.trimIndent()
    }

    private fun whereEntityTypeIdsAndEntitySetIds(entityTypeIds: Set<UUID>, entitySetIds: Set<UUID>? = null): String {
        val entityTypeClause = "${ENTITY_SETS.name}.${ENTITY_TYPE_ID.name} = ANY('{${entityTypeIds.joinToString()}}')"
        val entitySetClause = if (entitySetIds != null) {
            "AND ${ENTITY_SETS.name}.${ID.name} = ANY('{${entitySetIds.joinToString()}}')"
        } else {
            ""
        }

        return "$entityTypeClause $entitySetClause"
    }

    private fun reserveEntitySetIfNotExists(entitySet: EntitySet): UUID {
        aclKeyReservations.reserveIdAndValidateType(entitySet)

        checkState(entitySets.putIfAbsent(entitySet.id, entitySet) == null, "Entity set already exists.")
        dataSetService.initializeMetadata(AclKey(entitySet.id), SecurableObjectMetadata.fromEntitySet(entitySet))
        return entitySet.id
    }

    private fun setupDefaultEntitySetPropertyMetadata(entitySetId: UUID, entityTypeId: UUID) {
        val entityType = edm.getEntityType(entityTypeId)
        val propertyTags = entityType.propertyTags
        val propertyTypes = edm.getPropertyTypes(entityType.properties)

        dataSetService.initializeMetadata(propertyTypes.associate {
            val flags = propertyTags.getOrDefault(it.id, LinkedHashSet())
            AclKey(entitySetId, it.id) to SecurableObjectMetadata.fromPropertyType(it, flags)
        })
    }

    private fun ensureValidEntitySet(entitySet: EntitySet) {
        checkArgument(
                entityTypes.containsKey(entitySet.entityTypeId),
                "Entity Set Type does not exists."
        )

        if (entitySet.isLinking) {
            entitySet.linkedEntitySets.forEach { linkedEntitySetId ->
                checkArgument(
                        getEntityTypeByEntitySetId(linkedEntitySetId).id == entitySet.entityTypeId,
                        "Entity type of linked entity sets must be the same as of the linking entity set."
                )
                checkArgument(
                        !getEntitySet(linkedEntitySetId)!!.isLinking,
                        "Cannot add linking entity set as linked entity set."
                )
            }
        }
    }

    @Timed
    override fun deleteEntitySet(entitySet: EntitySet) {
        // If this entity set is linked to a linking entity set, we need to collect all the linking ids of the entity
        // set first in order to be able to reindex those, before entity data is unavailable
        if (!entitySet.isLinking) {
            checkAndRemoveEntitySetLinks(entitySet.id)
        }

        val entityType = edm.getEntityType(entitySet.entityTypeId)
        deleteEntitySet(entitySet, entityType)

        eventBus.post(EntitySetDeletedEvent(entitySet.id, entityType.id))
        dataSetService.deleteObjectMetadata(AclKey(entitySet.id))
        logger.info("Entity set ${entitySet.name} (${entitySet.id}) deleted successfully.")
    }

    private fun deleteEntitySet(entitySet: EntitySet, entityType: EntityType) {
        /*
         * We cleanup permissions first as this will make entity set unavailable, even if delete fails.
         */
        authorizations.deletePermissions(AclKey(entitySet.id))
        entityType.properties
                .map { propertyTypeId -> AclKey(entitySet.id, propertyTypeId) }
                .forEach { aclKey ->
                    authorizations.deletePermissions(aclKey)
                }

        aclKeyReservations.release(entitySet.id)
        dataSetService.deleteObjectMetadata(AclKey(entitySet.id))
        entitySets.delete(entitySet.id)
        deletedEntitySets[entitySet.id] = DelegatedIntSet(entitySet.partitions)
    }

    /**
     * Checks and removes links if deleted entity set is linked to a linking entity set
     *
     * @param entitySetId the id of the deleted entity set
     */
    private fun checkAndRemoveEntitySetLinks(entitySetId: UUID) {
        edm.getAllLinkingEntitySetIdsForEntitySet(entitySetId).forEach { linkingEntitySetId ->
            removeLinkedEntitySets(linkingEntitySetId, setOf(entitySetId))
            logger.info(
                    "Removed link between linking entity set ($linkingEntitySetId) and deleted entity set " +
                            "($entitySetId)"
            )
        }
    }

    private val GET_ENTITY_SET_COUNT = """
        SELECT ${PostgresColumn.COUNT} 
            FROM ${PostgresEntitySetSizesInitializationTask.ENTITY_SET_SIZES_VIEW} 
            WHERE ${PostgresColumn.ENTITY_SET_ID.name} = ?
    """.trimIndent()

    override fun getEntitySetSize(entitySetId: UUID): Long {
        return hds.connection.use { connection ->
            connection.prepareStatement(GET_ENTITY_SET_COUNT).use { ps ->
                ps.setObject(1, entitySetId)
                ps.executeQuery().use { rs ->
                    if (rs.next()) {
                        rs.getLong(1)
                    } else {
                        0
                    }
                }
            }
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
        return entitySets.values
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

    @Suppress("UNCHECKED_CAST")
    override fun getEntitySetIdsWithFlags(entitySetIds: Set<UUID>, filteringFlags: Set<EntitySetFlag>): Set<UUID> {
        return entitySets.aggregate(
                EntitySetsFlagFilteringAggregator(filteringFlags),
                Predicates.`in`(
                        QueryConstants.KEY_ATTRIBUTE_NAME.value(),
                        *entitySetIds.toTypedArray()
                )
        )
    }

    @Timed
    override fun getEntitySetsForOrganization(organizationId: UUID): Set<UUID> {
        return entitySets.keySet(Predicates.equal(EntitySetMapstore.ORGANIZATION_INDEX, organizationId))
    }

    @Timed
    override fun filterEntitySetsForOrganization(organizationId: UUID, entitySetIds: Collection<UUID>): Set<UUID> {
        return entitySets.keySet(
                Predicates.and(
                        Predicates.equal<UUID, EntitySet>(EntitySetMapstore.ORGANIZATION_INDEX, organizationId),
                        Predicates.`in`<UUID, EntitySet>(EntitySetMapstore.ID_INDEX, *entitySetIds.toTypedArray())
                )
        )
    }

    override fun getEntityTypeByEntitySetId(entitySetId: UUID): EntityType {
        val entityTypeId = getEntitySet(entitySetId)!!.entityTypeId
        return edm.getEntityType(entityTypeId)
    }

    @Suppress("UNCHECKED_CAST")
    override fun getEntityTypeIdsByEntitySetIds(entitySetIds: Set<UUID>): Map<UUID, UUID> {
        return entitySets.executeOnKeys(entitySetIds, GetEntityTypeFromEntitySetEntryProcessor()) as Map<UUID, UUID>
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
        return entitySets.executeOnKeys(
                entitySetIds,
                EntitySetContainsFlagEntryProcessor(flag)
        ).values.any { it as Boolean }
    }

    @Timed
    @Suppress("UNCHECKED_CAST")
    override fun filterToAuthorizedNormalEntitySets(
            entitySetIds: Set<UUID>,
            permissions: EnumSet<Permission>,
            principals: Set<Principal>
    ): Set<UUID> {

        val accessChecks = mutableMapOf<AclKey, EnumSet<Permission>>()

        val entitySetIdToNormalEntitySetIds = entitySets.executeOnKeys(
                entitySetIds,
                GetNormalEntitySetIdsEntryProcessor()
        ).mapValues {
            val set = (it.value as DelegatedUUIDSet).unwrap()
            set.forEach { id ->
                accessChecks.putIfAbsent(AclKey(id), permissions)
            }
            set
        }

        val entitySetsToAuthorizedStatus = authorizations.authorize(accessChecks, principals)
                .map { it.key.first() to it.value.values.all { bool -> bool } }.toMap()

        return entitySetIdToNormalEntitySetIds.filterValues {
            it.all { id ->
                entitySetsToAuthorizedStatus.getValue(
                        id
                )
            }
        }.keys
    }

    @Timed
    override fun getPropertyTypesOfEntitySets(entitySetIds: Set<UUID>): Map<UUID, Map<UUID, PropertyType>> {
        val entityTypesOfEntitySets = getEntityTypeIdsByEntitySetIds(entitySetIds)
        val missingEntitySetIds = entitySetIds - entityTypesOfEntitySets.keys

        check(missingEntitySetIds.isEmpty()) { "Missing the following entity set ids: $missingEntitySetIds" }

        val entityTypesAsMap = edm.getEntityTypesAsMap(entityTypesOfEntitySets.values.toSet())
        val propertyTypesAsMap = edm.getPropertyTypesAsMap(
                entityTypesAsMap.values.map { it.properties }.flatten().toSet()
        )

        return entitySetIds.associateWith {
            entityTypesAsMap.getValue(entityTypesOfEntitySets.getValue(it))
                    .properties
                    .associateWith { ptId -> propertyTypesAsMap.getValue(ptId) }
        }
    }

    @Timed
    override fun exists(entitySetId: UUID): Boolean = entitySets.containsKey(entitySetId)

    @Timed
    @Suppress("UNCHECKED_CAST")
    override fun getPropertyTypesForEntitySet(entitySetId: UUID): Map<UUID, PropertyType> {
        val maybeEtId = entitySets.executeOnKey(entitySetId, GetEntityTypeFromEntitySetEntryProcessor())
                as? UUID
                ?: throw  ResourceNotFoundException("Entity set $entitySetId does not exist.")

        val maybeEtProps = entityTypes.executeOnKey(maybeEtId, GetPropertiesFromEntityTypeEntryProcessor())
                as? DelegatedUUIDSet
                ?: throw  ResourceNotFoundException("Entity type $maybeEtId does not exist.")

        return propertyTypes.getAll(maybeEtProps)
    }

    @Timed
    override fun getEntitySetPropertyMetadata(entitySetId: UUID, propertyTypeId: UUID): EntitySetPropertyMetadata {
        val aclKey = AclKey(entitySetId, propertyTypeId)

        var metadata = objectMetadata[aclKey]
        if (metadata == null) {
            val entityTypeId = entitySets.executeOnKey(entitySetId, GetEntityTypeFromEntitySetEntryProcessor())
            val tags = entityTypes.getValue(entityTypeId).propertyTags[propertyTypeId] ?: mutableSetOf<String>()
            metadata = SecurableObjectMetadata.fromPropertyType(propertyTypes.getValue(propertyTypeId), tags)
            objectMetadata[aclKey] = metadata
        }

        return EntitySetPropertyMetadata(
                metadata.title,
                metadata.description,
                metadata.flags.mapTo(linkedSetOf()) { it },
                true
        )
    }

    @Timed
    override fun getAllEntitySetPropertyMetadata(entitySetId: UUID): Map<UUID, EntitySetPropertyMetadata> {
        return getEntityTypeByEntitySetId(entitySetId).properties.associateWith {
            getEntitySetPropertyMetadata(entitySetId, it)
        }
    }

    @Timed
    @Suppress("UNCHECKED_CAST")
    override fun getAllEntitySetPropertyMetadataForIds(
            entitySetIds: Set<UUID>
    ): Map<UUID, Map<UUID, EntitySetPropertyMetadata>> {
        val entityTypesByEntitySetId = entitySets.executeOnKeys(
                entitySetIds,
                GetEntityTypeFromEntitySetEntryProcessor()
        ) as Map<UUID, UUID>
        val entityTypesById = entityTypes.getAll(entityTypesByEntitySetId.values.toSet())

        val keys = entitySetIds
                .flatMap { entitySetId ->
                    entityTypesById.getValue(entityTypesByEntitySetId.getValue(entitySetId)).properties
                            .map { propertyTypeId ->
                                AclKey(entitySetId, propertyTypeId)
                            }
                }
                .toSet()

        val metadataMap = objectMetadata.getAll(keys)

        val missingKeys = Sets.difference(keys, metadataMap.keys).toSet()
        val missingPropertyTypesById = propertyTypes.getAll(missingKeys.mapTo(mutableSetOf()) { it[1] })

        missingKeys.forEach { newKey ->
            val propertyType = missingPropertyTypesById.getValue(newKey[1])
            val propertyTags = entityTypesById.getValue(entityTypesByEntitySetId.getValue(newKey[0]))
                    .propertyTags.getOrDefault(newKey[1], LinkedHashSet())

            val defaultMetadata = SecurableObjectMetadata.fromPropertyType(propertyType, propertyTags)

            metadataMap[newKey] = defaultMetadata
            objectMetadata[newKey] = defaultMetadata
        }

        return metadataMap.entries
                .groupBy { it.key[0] }
                .mapValues {
                    it.value.associateBy({ e -> e.key[1] }, { e ->
                        EntitySetPropertyMetadata(
                                e.value.title,
                                e.value.description,
                                e.value.flags.mapTo(linkedSetOf()) { f -> f },
                                true
                        )
                    })
                }
    }

    @Timed
    override fun updateEntitySetPropertyMetadata(entitySetId: UUID, propertyTypeId: UUID, update: MetadataUpdate) {
        val key = AclKey(entitySetId, propertyTypeId)
        dataSetService.updateObjectMetadata(key, SecurableObjectMetadataUpdate.fromMetadataUpdate(update))
    }

    @Timed
    override fun updateEntitySet(entitySetId: UUID, update: MetadataUpdate) {
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

        val newEntitySet = updateEntitySetMetadata(entitySetId, update)
        dataSetService.updateObjectMetadata(AclKey(entitySetId), SecurableObjectMetadataUpdate.fromMetadataUpdate(update))
        eventBus.post(EntitySetMetadataUpdatedEvent(newEntitySet))
    }

    @Timed
    override fun updateEntitySetMetadata(entitySetId: UUID, update: MetadataUpdate): EntitySet {
        return entitySets.executeOnKey(entitySetId, UpdateEntitySetMetadataProcessor(update))
    }

    @Timed
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

    @Timed
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

    @Timed
    override fun getLinkedEntitySets(entitySetId: UUID): Set<EntitySet> {
        val linkedEntitySetIds = getEntitySet(entitySetId)!!.linkedEntitySets
        return entitySets.getAll(linkedEntitySetIds).values.toSet()
    }

    override fun getLinkedEntitySetIds(entitySetId: UUID): Set<UUID> {
        return entitySets.executeOnKey(entitySetId) {
            DelegatedUUIDSet.wrap(
                    it.value?.linkedEntitySets ?: mutableSetOf()
            )
        }
    }

    override fun removeDataExpirationPolicy(entitySetId: UUID) {
        entitySets.executeOnKey(entitySetId, RemoveDataExpirationPolicyProcessor())
    }

    override fun getAuditRecordEntitySetsManager(): AuditRecordEntitySetsManager {
        return aresManager
    }
}
