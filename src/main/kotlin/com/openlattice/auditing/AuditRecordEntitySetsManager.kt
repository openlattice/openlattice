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

import com.openlattice.authorization.Principal
import com.openlattice.datastore.services.EdmManager
import com.openlattice.datastore.services.EdmService
import com.openlattice.edm.EntitySet
import com.openlattice.postgres.streams.PostgresIterable
import java.time.OffsetDateTime
import java.util.*

/**
 *
 *
 */
class AuditRecordEntitySetsManager(
        val auditingConfiguration: AuditingConfiguration,
        val edm: EdmManager
) {

    fun createAuditEntitySet(principal: Principal, entitySetId: UUID) {
        val auditedEntitySet = edm.getEntitySet(entitySetId)
        val entitySetName = buildName(entitySetId)
        val auditingEntityTypeId = auditingConfiguration.getAuditingEntityTypeId()

        val entitySet = EntitySet(
                auditingEntityTypeId,
                entitySetName,
                "Audit entity set for ${auditedEntitySet.name} (${auditedEntitySet.id})",
                Optional.of("This is an automatically generated auditing entity set."),
                auditedEntitySet.contacts
        )
        edm.createEntitySet(principal, entitySet)
    }

    fun getRecordEntitySets(entitySetId: UUID): PostgresIterable<UUID> {
        TODO("Implement retrieving all the audit record entity sets for a given entity set.")
    }

    fun rollAuditEntitySets() {
        TODO("Implement rolling of audit entity sets.")
    }

    private fun buildName(entitySetId: UUID): String {
        val odt = OffsetDateTime.now()
        return "$entitySetId-${odt.year}-${odt.monthValue}-${odt.dayOfMonth}-${System.currentTimeMillis()}"
    }

    fun getRecordEntitySet(entitySet: UUID): UUID {
        TODO("Implement retrieving the audit record entity set.")
    }
}