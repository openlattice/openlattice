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
import com.openlattice.authorization.AuthorizationManager
import com.openlattice.authorization.Principal
import com.openlattice.authorization.SecurablePrincipal
import com.openlattice.authorization.securable.AbstractSecurableObject
import com.openlattice.datastore.services.EdmManager
import com.openlattice.datastore.services.EdmService
import com.openlattice.edm.EntitySet
import com.openlattice.edm.processors.UpdateAuditRecordEntitySetIdProcessor
import com.openlattice.edm.type.EntityType
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.postgres.streams.PostgresIterable
import java.time.OffsetDateTime
import java.util.*

/**
 * This class keeps track of auditing entity sets for each entity set.
 *
 * This class should probably be merged into EdmService as there is an unbreakable circular dependency there.
 */
class AuditRecordEntitySetsManager(
        val auditingConfiguration: AuditingConfiguration,
        private val authorizationManager: AuthorizationManager,
        private val hazelcastInstance: HazelcastInstance,
        private val edm: EdmManager
) {

    private val auditRecordEntitySetConfigurations = hazelcastInstance.getMap<UUID, AuditRecordEntitySetConfiguration>(
            HazelcastMap.AUDIT_RECORD_ENTITY_SETS.name
    )

    fun createAuditEntitySetForEntitySet(principal: Principal, entitySetId: UUID) {
        val auditedEntitySet = edm.getEntitySet(entitySetId)
        val name = auditedEntitySet.name
        createAuditEntitySet( principal, name, entitySetId, auditedEntitySet.contacts)

    }

    fun createAuditEntitySetForSecurablePrincipal(principal: Principal, securableObject: AbstractSecurableObject) {
        val name = securableObject.title
        val id = securableObject.id
        val contacts = setOf<String>()
        createAuditEntitySet( principal, name, id, contacts)
    }

    fun createAuditEntitySet(principal: Principal, name: String, id: UUID, contacts: Set<String>) {

        val entitySetName = buildName(id)
        val auditingEntityTypeId = auditingConfiguration.getAuditingEntityTypeId()

        val entitySet = EntitySet(
                auditingEntityTypeId,
                entitySetName,
                "Audit entity set for $name ($id)",
                Optional.of("This is an automatically generated auditing entity set."),
                contacts
        )

        /*
         * This sequence of steps is safe to execute as a failure on any of the steps can be retried from scratch
         * with the only side-effect being that eventually we will have to clean-up audit record entity sets that were
         * never used.
         */

        edm.createEntitySet(principal, entitySet)
        auditRecordEntitySetConfigurations
                .executeOnKey(id, UpdateAuditRecordEntitySetIdProcessor(entitySet.id))

    }

    fun getAuditRecordEntitySets(entitySetId: UUID): Set<UUID> {
        return auditRecordEntitySetConfigurations[entitySetId]?.auditRecordEntitySetIds ?: setOf()
    }

    fun getActiveAuditRecordEntitySetId(entitySetId: UUID): UUID {
        //TODO: Don't load the entire object
        //This should never NPE as (almost) every securable object should have an entity set.
        return auditRecordEntitySetConfigurations[entitySetId]!!.activeAuditRecordEntitySetId
    }

    fun rollAuditEntitySets() {
        TODO("Implement rolling of audit entity sets.")
    }

    private fun buildName(entitySetId: UUID): String {
        val odt = OffsetDateTime.now()
        return "$entitySetId-${odt.year}-${odt.monthValue}-${odt.dayOfMonth}-${System.currentTimeMillis()}"
    }
}