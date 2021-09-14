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

package com.openlattice.auditing

import com.google.common.collect.ImmutableSet
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.query.Predicate
import com.hazelcast.query.Predicates
import com.openlattice.authorization.*
import com.openlattice.data.storage.partitions.PartitionManager
import com.openlattice.datastore.services.EntitySetManager
import com.openlattice.edm.EntitySet
import com.openlattice.edm.processors.CreateOrUpdateAuditRecordEntitySetsProcessor
import com.openlattice.edm.processors.UpdateAuditEdgeEntitySetIdProcessor
import com.openlattice.edm.set.EntitySetFlag
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.organization.OrganizationExternalDatabaseTable
import com.openlattice.postgres.mapstores.AuditRecordEntitySetConfigurationMapstore.ANY_AUDITING_ENTITY_SETS
import com.openlattice.postgres.mapstores.AuditRecordEntitySetConfigurationMapstore.ANY_EDGE_AUDITING_ENTITY_SETS
import org.slf4j.LoggerFactory
import java.time.OffsetDateTime
import java.util.*
import kotlin.collections.LinkedHashSet

const val EDM_AUDIT_ENTITY_SET_NAME = "edm_audit_entity_set"

private val logger = LoggerFactory.getLogger(AuditRecordEntitySetsManager::class.java)

/**
 * This class keeps track of auditing entity sets for each entity set.
 *
 * This class should probably be merged into EdmService as there is an unbreakable circular dependency there.
 */

class AuditRecordEntitySetsManager(
        val auditingTypes: AuditingTypes,
        private val entitySetManager: EntitySetManager,
        private val partitionManager: PartitionManager,
        private val authorizationManager: AuthorizationManager,
        hazelcastInstance: HazelcastInstance

) {
    private val securableObjectTypes = HazelcastMap.SECURABLE_OBJECT_TYPES.getMap(hazelcastInstance)
    private val auditRecordEntitySetConfigurations = HazelcastMap.AUDIT_RECORD_ENTITY_SETS.getMap(hazelcastInstance)

    private val organizations = HazelcastMap.ORGANIZATIONS.getMap(hazelcastInstance)

    private val edmAuditTypes = setOf(
            AuditEventType.CREATE_PROPERTY_TYPE,
            AuditEventType.UPDATE_PROPERTY_TYPE,
            AuditEventType.DELETE_PROPERTY_TYPE,
            AuditEventType.CREATE_ENTITY_TYPE,
            AuditEventType.UPDATE_ENTITY_TYPE,
            AuditEventType.ADD_PROPERTY_TYPE_TO_ENTITY_TYPE,
            AuditEventType.REMOVE_PROPERTY_TYPE_FROM_ENTITY_TYPE,
            AuditEventType.DELETE_ENTITY_TYPE,
            AuditEventType.CREATE_ASSOCIATION_TYPE,
            AuditEventType.UPDATE_ASSOCIATION_TYPE,
            AuditEventType.ADD_ENTITY_TYPE_TO_ASSOCIATION_TYPE,
            AuditEventType.REMOVE_ENTITY_TYPE_FROM_ASSOCIATION_TYPE,
            AuditEventType.DELETE_ASSOCIATION_TYPE
    )

    private var edmAuditEntitySetId: UUID? = null

    fun createAuditEntitySetForEntitySet(auditedEntitySet: EntitySet) {
        if (auditedEntitySet.isAudit) {
            return
        }

        if (auditingTypes.isAuditingInitialized()) {
            val name = auditedEntitySet.name
            createAuditEntitySets(
                    name,
                    AclKey(auditedEntitySet.id),
                    auditedEntitySet.contacts,
                    auditedEntitySet.organizationId,
                    auditedEntitySet.partitions
            )
        }

    }

    fun createAuditEntitySetForOrganization(organizationId: UUID) {
        if (auditingTypes.isAuditingInitialized()) {
            val name = organizations[organizationId]!!.title
            createAuditEntitySets(name, AclKey(organizationId), ImmutableSet.of(), organizationId)
        }
    }

    fun createAuditEntitySetForExternalDBTable(table: OrganizationExternalDatabaseTable) {
        if (auditingTypes.isAuditingInitialized()) {
            val name = table.name
            createAuditEntitySets(name, AclKey(table.id), setOf(), table.organizationId)
        }
    }

    private fun createAuditEntitySets(
            name: String,
            aclKey: AclKey,
            contacts: Set<String>,
            organizationId: UUID,
            partitions: Set<Int> = LinkedHashSet()
    ) {
        createAuditEntitySets(
                aclKey,
                buildAuditEntitySet(name, aclKey, contacts, organizationId, partitions),
                buildAuditEdgeEntitySet(name, aclKey, contacts, organizationId, partitions)
        )
    }

    private fun createAuditEntitySets(aclKey: AclKey, entitySet: EntitySet, edgeEntitySet: EntitySet) {

        val ownerPrincipals = authorizationManager.getAuthorizedPrincipalsOnSecurableObject(
                aclKey, EnumSet.of(Permission.OWNER)
        )

        val firstUserPrincipal: Principal

        try {
            firstUserPrincipal = ownerPrincipals.firstOrNull { it.type == PrincipalType.USER }
                    ?: ownerPrincipals.first { it.type == PrincipalType.ORGANIZATION }
        } catch (e: NoSuchElementException) {
            logger.error("Unable to create audit entity set for securable object {} because it has no owner", aclKey)
            return
        }

        /*
         * This sequence of steps is safe to execute as a failure on any of the steps can be retried from scratch
         * with the only side-effect being that eventually we will have to clean-up audit record entity sets that were
         * never used.
         */

        val aclKeyRoot = aclKey.first()
        if (auditRecordEntitySetConfigurations.keySet(isAnAuditEntitySetPredicate(aclKeyRoot)).isEmpty()) {
            auditRecordEntitySetConfigurations
                    .executeOnKey(aclKey, CreateOrUpdateAuditRecordEntitySetsProcessor(entitySet.id, edgeEntitySet.id))
            entitySetManager.createEntitySet(firstUserPrincipal, entitySet)
            entitySetManager.createEntitySet(firstUserPrincipal, edgeEntitySet)
        }

        val auditAclKeys = auditingTypes.propertyTypeIds.values.map { AclKey(entitySet.id, it) }.toMutableSet()
        auditAclKeys.add(AclKey(entitySet.id))

        authorizationManager.setPermission(auditAclKeys, ownerPrincipals, EnumSet.allOf(Permission::class.java))
    }

    fun initializeAuditEdgeEntitySet(aclKey: AclKey) {
        if (!auditingTypes.isAuditingInitialized()) {
            return
        }
        val aclKeyRoot = aclKey.first()

        if (auditRecordEntitySetConfigurations.keySet(isAnAuditEntitySetPredicate(aclKeyRoot)).isEmpty()) {

            val auditEntitySetId = auditRecordEntitySetConfigurations.getValue(aclKey).activeAuditRecordEntitySetId
            val auditEntitySet = entitySetManager.getEntitySet(auditEntitySetId)!!
            val currentAuditEntitySetAclKey = AclKey(auditEntitySetId)

            val newAuditEdgeEntitySet = buildAuditEdgeEntitySet(
                    securableObjectTypes[aclKey]?.name ?: "Missing Type",
                    aclKey,
                    auditEntitySet.contacts,
                    auditEntitySet.organizationId,
                    auditEntitySet.partitions
            )
            val newEdgeAclKey = AclKey(newAuditEdgeEntitySet.id)

            auditRecordEntitySetConfigurations
                    .executeOnKey(aclKey, UpdateAuditEdgeEntitySetIdProcessor(newAuditEdgeEntitySet.id))
            val ownerPrincipals = authorizationManager.getAuthorizedPrincipalsOnSecurableObject(
                    aclKey, EnumSet.of(Permission.OWNER)
            )

            val firstUserPrincipal: Principal

            try {
                firstUserPrincipal = ownerPrincipals.first { it.type == PrincipalType.USER }
            } catch (e: NoSuchElementException) {
                logger.error(
                        "Unable to create audit entity set for securable object {} because it has no owner", aclKey
                )
                return
            }

            entitySetManager.createEntitySet(firstUserPrincipal, newAuditEdgeEntitySet)
            authorizationManager.getAllSecurableObjectPermissions(currentAuditEntitySetAclKey).aces.forEach { ace ->
                authorizationManager.addPermission(newEdgeAclKey, ace.principal, ace.permissions, ace.expirationDate)
            }
        }
    }

    fun getAuditRecordEntitySets(aclKey: AclKey): Set<UUID> {
        return auditRecordEntitySetConfigurations[aclKey]?.auditRecordEntitySetIds?.toSet() ?: setOf()
    }

    fun getAuditEdgeEntitySets(aclKey: AclKey): Set<UUID> {
        return auditRecordEntitySetConfigurations[aclKey]?.auditEdgeEntitySetIds?.toSet() ?: setOf()
    }

    fun removeAuditRecordEntitySetConfiguration(aclKey: AclKey) {
        auditRecordEntitySetConfigurations.delete(aclKey)
    }

    fun getActiveAuditEntitySetIds(aclKey: AclKey, eventType: AuditEventType): AuditEntitySetsConfiguration {
        val aclKeyRoot = aclKey.first() // TODO do we always only care about the base id?

        if (edmAuditTypes.contains(eventType) || aclKeyRoot == getEdmAuditEntitySetId()) {
            return AuditEntitySetsConfiguration(getEdmAuditEntitySetId(), null)
        }

        val auditEntitySetConfiguration = auditRecordEntitySetConfigurations[AclKey(aclKeyRoot)]

        return if (auditEntitySetConfiguration == null) {

            val auditSets = auditRecordEntitySetConfigurations.values(
                    Predicates.equal(ANY_AUDITING_ENTITY_SETS, aclKeyRoot)
            )
            val auditEdgeSets = auditRecordEntitySetConfigurations.values(
                    Predicates.equal(ANY_EDGE_AUDITING_ENTITY_SETS, aclKeyRoot)
            )

            if (auditSets.isNotEmpty()) {
                return AuditEntitySetsConfiguration(aclKeyRoot, auditSets.first().activeAuditEdgeEntitySetId)
            }
            if (auditEdgeSets.isNotEmpty()) {
                return AuditEntitySetsConfiguration(auditEdgeSets.first().activeAuditRecordEntitySetId, aclKeyRoot)
            }
            logger.error("Missing audit entity set id for aclKey {}", aclKey)
            AuditEntitySetsConfiguration(null, null)
        } else {
            AuditEntitySetsConfiguration(
                    auditEntitySetConfiguration.activeAuditRecordEntitySetId,
                    auditEntitySetConfiguration.activeAuditEdgeEntitySetId
            )
        }


    }

    fun getActiveAuditRecordEntitySetId(aclKey: AclKey, eventType: AuditEventType): UUID? {
        //TODO: Don't load the entire object
        //This should never NPE as (almost) every securable object should have an entity set.

        if (edmAuditTypes.contains(eventType)) {
            return getEdmAuditEntitySetId()
        }

        val aclKeyRoot = aclKey.first() // TODO do we always only care about the base id?

        if (aclKeyRoot == getEdmAuditEntitySetId() || auditRecordEntitySetConfigurations.keySet(
                        Predicates.equal(ANY_AUDITING_ENTITY_SETS, aclKeyRoot)
                ).isNotEmpty()) {
            return aclKeyRoot
        }

        val auditEntitySetId = auditRecordEntitySetConfigurations[AclKey(aclKeyRoot)]?.activeAuditRecordEntitySetId

        if (auditEntitySetId == null) {
            logger.error("Missing audit entity set id for aclKey {}", aclKey)
        }

        return auditEntitySetId
    }

    fun rollAuditEntitySet(aclKey: AclKey) {
        val auditEntitySetId = auditRecordEntitySetConfigurations.getValue(aclKey).activeAuditRecordEntitySetId
        val auditEdgeEntitySetId = auditRecordEntitySetConfigurations.getValue(aclKey).activeAuditEdgeEntitySetId

        val auditEntitySet = entitySetManager.getEntitySet(auditEntitySetId)!!
        val auditEdgeEntitySet = entitySetManager.getEntitySet(auditEdgeEntitySetId!!)

        val currentAuditEntitySetAclKey = AclKey(auditEntitySetId)
        val currentAuditEdgeEntitySetAclKey = AclKey(auditEdgeEntitySetId)

        val currentAuditEntitySetPropertyTypeAclKeys = auditingTypes.propertyTypeIds.values
                .map { it to AclKey(auditEntitySetId, it) }
                .toMap()

        val newAuditEntitySet = buildAuditEntitySet(
                securableObjectTypes[aclKey]?.name ?: "Missing Type",
                aclKey,
                auditEntitySet.contacts,
                auditEntitySet.organizationId,
                auditEntitySet.partitions
        )

        val newAuditEdgeEntitySet = buildAuditEdgeEntitySet(
                securableObjectTypes[aclKey]?.name ?: "Missing Type",
                aclKey,
                auditEntitySet.contacts,
                auditEntitySet.organizationId,
                auditEntitySet.partitions
        )

        createAuditEntitySets(aclKey, newAuditEntitySet, newAuditEdgeEntitySet)

        val newAclKey = AclKey(newAuditEntitySet.id)
        val newEdgeAclKey = AclKey(newAuditEdgeEntitySet.id)
        val propertyTypeAclKeys = auditingTypes.propertyTypeIds.values.map {
            AclKey(
                    newAuditEntitySet.id, it
            )
        } //TODO: Why is this not used?

        //Since the edge entity set has no properties, we only copy entity set permissions
        //Copy over existing permissions for entity set metadata
        authorizationManager.getAllSecurableObjectPermissions(currentAuditEntitySetAclKey).aces.forEach { ace ->
            authorizationManager.addPermission(newAclKey, ace.principal, ace.permissions, ace.expirationDate)
        }

        authorizationManager.getAllSecurableObjectPermissions(currentAuditEdgeEntitySetAclKey).aces.forEach { ace ->
            authorizationManager.addPermission(newEdgeAclKey, ace.principal, ace.permissions, ace.expirationDate)
        }

        //Copy over existing permissions for entity property types
        currentAuditEntitySetPropertyTypeAclKeys.mapValues { (propertyTypeId, currentPropertyTypeAclKey) ->
            authorizationManager.getAllSecurableObjectPermissions(currentPropertyTypeAclKey).aces.forEach { ace ->
                authorizationManager.addPermission(
                        AclKey(newAuditEntitySet.id, propertyTypeId),
                        ace.principal,
                        ace.permissions,
                        ace.expirationDate
                )
            }
        }
    }

    private fun buildAuditEdgeEntitySet(
            name: String,
            aclKey: AclKey,
            contacts: Set<String>,
            organizationId: UUID,
            partitions: Set<Int>
    ): EntitySet {
        val entitySetName = buildName(aclKey) + "_edges"
        val auditingEdgeEntityTypeId = auditingTypes.auditingEdgeEntityTypeId

        val entitySet = EntitySet(
                entityTypeId = auditingEdgeEntityTypeId,
                name = entitySetName,
                _title = "Audit edge entity set for $name ($aclKey)",
                _description = "This is an automatically generated auditing entity set.",
                contacts = contacts.toMutableSet(),
                organizationId = organizationId,
                flags = EnumSet.of(EntitySetFlag.AUDIT),
                partitions = partitions as LinkedHashSet<Int>
        )

        if (partitions.isEmpty()) {
            return partitionManager.allocateAllPartitions(entitySet)
        }

        return entitySet
    }

    private fun buildAuditEntitySet(
            name: String,
            aclKey: AclKey,
            contacts: Set<String>,
            organizationId: UUID,
            partitions: Set<Int>
    ): EntitySet {
        val entitySetName = buildName(aclKey)
        val auditingEntityTypeId = auditingTypes.auditingEntityTypeId

        val entitySet = EntitySet(
                entityTypeId = auditingEntityTypeId,
                name = entitySetName,
                _title = "Audit entity set for $name ($aclKey)",
                _description = "This is an automatically generated auditing entity set.",
                contacts = contacts.toMutableSet(),
                organizationId = organizationId,
                flags = EnumSet.of(EntitySetFlag.AUDIT),
                partitions = partitions as LinkedHashSet<Int>
        )

        if (partitions.isEmpty()) {
            return partitionManager.allocateAllPartitions(entitySet)
        }

        return entitySet
    }

    private fun buildName(aclKey: AclKey): String {
        val odt = OffsetDateTime.now()
        val aclKeyString = aclKey.joinToString("-")
        return "$aclKeyString-${odt.year}-${odt.monthValue}-${odt.dayOfMonth}-${System.currentTimeMillis()}"
    }

    private fun getEdmAuditEntitySetId(): UUID {
        if (edmAuditEntitySetId == null) {
            edmAuditEntitySetId = entitySetManager.getEntitySet(EDM_AUDIT_ENTITY_SET_NAME)!!.id
        }

        return edmAuditEntitySetId!!
    }

}

private fun isAnAuditEntitySetPredicate(entitySetId: UUID): Predicate<AclKey, AuditRecordEntitySetConfiguration> {
    return Predicates.or(
            Predicates.equal<AclKey, AuditRecordEntitySetConfiguration>(ANY_AUDITING_ENTITY_SETS, entitySetId),
            Predicates.equal<AclKey, AuditRecordEntitySetConfiguration>(ANY_EDGE_AUDITING_ENTITY_SETS, entitySetId)
    )
}