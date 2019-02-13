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

import com.hazelcast.core.HazelcastInstance
import com.hazelcast.query.Predicates
import com.openlattice.authorization.AclKey
import com.openlattice.authorization.AuthorizationManager
import com.openlattice.authorization.Principal
import com.openlattice.authorization.securable.AbstractSecurableObject
import com.openlattice.authorization.securable.SecurableObjectType
import com.openlattice.datastore.services.EdmManager
import com.openlattice.edm.EntitySet
import com.openlattice.edm.processors.UpdateAuditRecordEntitySetIdProcessor
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.postgres.mapstores.AuditRecordEntitySetConfigurationMapstore
import com.openlattice.postgres.mapstores.AuditRecordEntitySetConfigurationMapstore.*
import java.time.OffsetDateTime
import java.util.*

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

    fun createAuditEntitySetForEntitySet(principal: Principal, entitySetId: UUID) {
        if (auditingTypes.isAuditingInitialized()) {
            val auditedEntitySet = edm.getEntitySet(entitySetId)
            val name = auditedEntitySet.name
            createAuditEntitySet(principal, name, AclKey(entitySetId), auditedEntitySet.contacts)
        }

    }

    fun createAuditEntitySet(principal: Principal, name: String, aclKey: AclKey, contacts: Set<String>) {
        createAuditEntitySet(principal, aclKey, buildAuditEntitySet(name, aclKey, contacts))
    }

    fun createAuditEntitySet(principal: Principal, aclKey: AclKey, entitySet: EntitySet) {

        /*
         * This sequence of steps is safe to execute as a failure on any of the steps can be retried from scratch
         * with the only side-effect being that eventually we will have to clean-up audit record entity sets that were
         * never used.
         */

        val securableObjectId = aclKey.first()
        if (auditRecordEntitySetConfigurations.keySet(Predicates.equal(ANY_AUDITING_ENTITY_SETS, securableObjectId)).isEmpty()) {
            auditRecordEntitySetConfigurations
                    .executeOnKey(aclKey, UpdateAuditRecordEntitySetIdProcessor(entitySet.id))
            edm.createEntitySet(principal, entitySet)
        }

    }

    fun getAuditRecordEntitySets(aclKey: AclKey): Set<UUID> {
        return auditRecordEntitySetConfigurations[aclKey]?.auditRecordEntitySetIds ?: setOf()
    }

    fun getActiveAuditRecordEntitySetId(aclKey: AclKey): UUID {
        //TODO: Don't load the entire object
        //This should never NPE as (almost) every securable object should have an entity set.
        if (aclKey.size == 1
                && securableObjectTypes[aclKey] == SecurableObjectType.EntitySet
                && auditRecordEntitySetConfigurations.keySet(Predicates.equal(ANY_AUDITING_ENTITY_SETS, aclKey.first())).isNotEmpty()) {
            return aclKey.first()
        }

        return auditRecordEntitySetConfigurations[aclKey]!!.activeAuditRecordEntitySetId
    }

    fun rollAuditEntitySet(principal: Principal, aclKey: AclKey) {
        val auditEntitySetId = auditRecordEntitySetConfigurations[aclKey]!!.activeAuditRecordEntitySetId
        val auditEntitySet = edm.getEntitySet(auditEntitySetId)
        var currentAuditEntitySetAclKey = AclKey(auditEntitySetId)
        var currentAuditEntitySetPropertyTypeAclKeys = auditingTypes.propertyTypeIds.values
                .map { it to AclKey(auditEntitySetId, it) }
                .toMap()

        val newAuditEntitySet = buildAuditEntitySet(
                securableObjectTypes[aclKey]?.name ?: "Missing Type",
                aclKey,
                auditEntitySet.contacts
        )

        createAuditEntitySet(principal, aclKey, newAuditEntitySet)
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
                        principal,
                        ace.permissions,
                        ace.expirationDate
                )
            }
        }
    }

    private fun buildAuditEntitySet(name: String, aclKey: AclKey, contacts: Set<String>): EntitySet {
        val entitySetName = buildName(aclKey)
        val auditingEntityTypeId = auditingTypes.auditingEntityTypeId

        return EntitySet(
                auditingEntityTypeId,
                entitySetName,
                "Audit entity set for $name ($aclKey)",
                Optional.of("This is an automatically generated auditing entity set."),
                contacts
        )
    }

    private fun buildName(aclKey: AclKey): String {
        val odt = OffsetDateTime.now()
        val aclKeyString = aclKey.joinToString("-")
        return "$aclKeyString-${odt.year}-${odt.monthValue}-${odt.dayOfMonth}-${System.currentTimeMillis()}"
    }
}