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

package com.openlattice.edm.processors

import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.auditing.AuditRecordEntitySetConfiguration
import com.openlattice.authorization.AclKey
import org.slf4j.LoggerFactory
import java.util.*


private val logger = LoggerFactory.getLogger(CreateOrUpdateAuditRecordEntitySetsProcessor::class.java)

/**
 * Entry processor for updating the audit entity set.
 */
data class CreateOrUpdateAuditRecordEntitySetsProcessor(
        val auditRecordEntitySetId: UUID,
        val auditEdgeEntitySetId: UUID
) : AbstractRhizomeEntryProcessor<AclKey, AuditRecordEntitySetConfiguration, Void?>() {
    override fun process(entry: MutableMap.MutableEntry<AclKey, AuditRecordEntitySetConfiguration?>): Void? {
        val config = entry.value

        if (config == null) {

            logger.debug("Initializing audit record entity set configuration for securable object id {}.", entry.key)
            entry.setValue(AuditRecordEntitySetConfiguration(auditRecordEntitySetId, auditEdgeEntitySetId))
        } else {

            config.activeAuditRecordEntitySetId = auditRecordEntitySetId
            config.auditRecordEntitySetIds.add(auditRecordEntitySetId)

            config.activeAuditEdgeEntitySetId = auditEdgeEntitySetId
            config.auditEdgeEntitySetIds.add(auditEdgeEntitySetId)

            entry.setValue(config)
        }

        return null
    }
}