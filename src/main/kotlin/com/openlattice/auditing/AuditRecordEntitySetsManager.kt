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
import com.hazelcast.query.Predicates
import com.openlattice.authorization.*
import com.openlattice.authorization.securable.SecurableObjectType
import com.openlattice.datastore.services.EdmManager
import com.openlattice.edm.EntitySet
import com.openlattice.edm.processors.UpdateAuditRecordEntitySetIdProcessor
import com.openlattice.edm.set.EntitySetFlag
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.postgres.mapstores.AuditRecordEntitySetConfigurationMapstore.ANY_AUDITING_ENTITY_SETS
import org.slf4j.LoggerFactory
import java.time.OffsetDateTime
import java.util.*

const val EDM_AUDIT_ENTITY_SET_NAME = "edm_audit_entity_set"

private val logger = LoggerFactory.getLogger(AuditRecordEntitySetsManager::class.java)

/**
 * This class keeps track of auditing entity sets for each entity set.
 *
 * This class should probably be merged into EdmService as there is an unbreakable circular dependency there.
 */

class AuditRecordEntitySetsManager(
        val auditingTypes: AuditingTypes,
        val edm: EdmManager,
        private val authorizationManager: AuthorizationManager,
        private val hazelcastInstance: HazelcastInstance

) {
    private val securableObjectTypes = hazelcastInstance.getMap<AclKey, SecurableObjectType>(
            HazelcastMap.SECURABLE_OBJECT_TYPES.name
    )
    private val auditRecordEntitySetConfigurations = hazelcastInstance.getMap<AclKey, AuditRecordEntitySetConfiguration>(
            HazelcastMap.AUDIT_RECORD_ENTITY_SETS.name
    )

    private val organizationTitles = hazelcastInstance.getMap<UUID, String>(
            HazelcastMap.ORGANIZATIONS_TITLES.name
    )

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
        if (auditedEntitySet.flags.contains(EntitySetFlag.AUDIT)) {
            return
        }

        if (auditingTypes.isAuditingInitialized()) {
            val name = auditedEntitySet.name
            createAuditEntitySet(name, AclKey(auditedEntitySet.id), auditedEntitySet.contacts, auditedEntitySet.organizationId)
        }

    }

    fun createAuditEntitySetForOrganization(organizationId: UUID) {
        if (auditingTypes.isAuditingInitialized()) {
            val name = organizationTitles[organizationId]!!
            createAuditEntitySet(name, AclKey(organizationId), ImmutableSet.of(), organizationId)
        }
    }

    fun createAuditEntitySet(name: String, aclKey: AclKey, contacts: Set<String>, organizationId: UUID) {
        createAuditEntitySet(aclKey, buildAuditEntitySet(name, aclKey, contacts, Optional.of(organizationId)))
    }

    fun createAuditEntitySet(aclKey: AclKey, entitySet: EntitySet) {

        val ownerPrincipals = authorizationManager.getAuthorizedPrincipalsOnSecurableObject(aclKey, EnumSet.of(Permission.OWNER))

        var firstUserPrincipal: Principal

        try {
            firstUserPrincipal = ownerPrincipals.first { it.type == PrincipalType.USER }
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
        if (auditRecordEntitySetConfigurations.keySet(Predicates.equal(ANY_AUDITING_ENTITY_SETS, aclKeyRoot)).isEmpty()) {
            auditRecordEntitySetConfigurations
                    .executeOnKey(aclKey, UpdateAuditRecordEntitySetIdProcessor(entitySet.id))
            edm.createEntitySet(firstUserPrincipal, entitySet)
        }

        val auditAclKeys = auditingTypes.propertyTypeIds.values.map{ AclKey(entitySet.id, it) }.toMutableSet()
        auditAclKeys.add(AclKey(entitySet.id))

        authorizationManager.setPermission(auditAclKeys, ownerPrincipals, EnumSet.allOf(Permission::class.java))

    }

    fun getAuditRecordEntitySets(aclKey: AclKey): Set<UUID> {
        return auditRecordEntitySetConfigurations[aclKey]?.auditRecordEntitySetIds ?: setOf()
    }

    fun getActiveAuditRecordEntitySetId(aclKey: AclKey, eventType: AuditEventType): UUID? {
        //TODO: Don't load the entire object
        //This should never NPE as (almost) every securable object should have an entity set.

        if (edmAuditTypes.contains(eventType)) {
            return getEdmAuditEntitySetId()
        }

        val aclKeyRoot = aclKey.first() // TODO do we always only care about the base id?

        if (aclKeyRoot == getEdmAuditEntitySetId() || auditRecordEntitySetConfigurations.keySet(Predicates.equal(ANY_AUDITING_ENTITY_SETS, aclKeyRoot)).isNotEmpty()) {
            return aclKeyRoot
        }

        val auditEntitySetId = auditRecordEntitySetConfigurations[AclKey(aclKeyRoot)]?.activeAuditRecordEntitySetId

        if (auditEntitySetId == null) {
            logger.error("Missing audit entity set id for aclKey {}", aclKey)
        }

        return auditEntitySetId
    }

    fun rollAuditEntitySet(aclKey: AclKey) {
        val auditEntitySetId = auditRecordEntitySetConfigurations[aclKey]!!.activeAuditRecordEntitySetId
        val auditEntitySet = edm.getEntitySet(auditEntitySetId)
        var currentAuditEntitySetAclKey = AclKey(auditEntitySetId)
        var currentAuditEntitySetPropertyTypeAclKeys = auditingTypes.propertyTypeIds.values
                .map { it to AclKey(auditEntitySetId, it) }
                .toMap()

        val newAuditEntitySet = buildAuditEntitySet(
                securableObjectTypes[aclKey]?.name ?: "Missing Type",
                aclKey,
                auditEntitySet.contacts,
                Optional.of(auditEntitySet.organizationId)
        )

        createAuditEntitySet(aclKey, newAuditEntitySet)
        val newAclKey = AclKey(newAuditEntitySet.id)
        val propertyTypeAclKeys = auditingTypes.propertyTypeIds.values.map { AclKey(newAuditEntitySet.id, it) }

        //Copy over existing permissions for entity set metadata
        authorizationManager.getAllSecurableObjectPermissions(currentAuditEntitySetAclKey).aces.forEach { ace ->
            authorizationManager.addPermission(newAclKey, ace.principal, ace.permissions, ace.expirationDate)
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

    private fun buildAuditEntitySet(name: String, aclKey: AclKey, contacts: Set<String>, organizationId: Optional<UUID>): EntitySet {
        val entitySetName = buildName(aclKey)
        val auditingEntityTypeId = auditingTypes.auditingEntityTypeId

        return EntitySet(
                auditingEntityTypeId,
                entitySetName,
                "Audit entity set for $name ($aclKey)",
                Optional.of("This is an automatically generated auditing entity set."),
                contacts,
                Optional.empty(),
                organizationId,
                Optional.of(EnumSet.of(EntitySetFlag.AUDIT))
        )
    }

    private fun buildName(aclKey: AclKey): String {
        val odt = OffsetDateTime.now()
        val aclKeyString = aclKey.joinToString("-")
        return "$aclKeyString-${odt.year}-${odt.monthValue}-${odt.dayOfMonth}-${System.currentTimeMillis()}"
    }

    private fun getEdmAuditEntitySetId(): UUID {
        if (edmAuditEntitySetId == null) {
            edmAuditEntitySetId = edm.getEntitySet(EDM_AUDIT_ENTITY_SET_NAME).id
        }

        return edmAuditEntitySetId!!
    }

}