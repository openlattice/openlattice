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


private val logger = LoggerFactory.getLogger(UpdateAuditRecordEntitySetIdProcessor::class.java)

/**
 * Entry processor for updating the audit entity set.
 */
class UpdateAuditRecordEntitySetIdProcessor(
        val auditRecordEntitySetId: UUID
) : AbstractRhizomeEntryProcessor<AclKey, AuditRecordEntitySetConfiguration, Void?>() {
    override fun process(entry: MutableMap.MutableEntry<AclKey, AuditRecordEntitySetConfiguration?>): Void? {
        val config = entry.value
        if (config == null) {
            logger.error(
                    "Encountered unexpected null value when updating audit record entity set id for securable object id {}.",
                    entry.key
            )
            entry.setValue(AuditRecordEntitySetConfiguration(auditRecordEntitySetId, mutableSetOf(auditRecordEntitySetId)))
        } else {
            config.activeAuditRecordEntitySetId = auditRecordEntitySetId
            config.auditRecordEntitySetIds.add(auditRecordEntitySetId)
            entry.setValue(config)
        }

        return null
    }
}