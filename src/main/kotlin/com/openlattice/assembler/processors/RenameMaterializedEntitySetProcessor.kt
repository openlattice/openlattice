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
package com.openlattice.assembler.processors

import com.hazelcast.core.Offloadable
import com.hazelcast.spi.impl.executionservice.ExecutionService
import com.openlattice.assembler.AssemblerConnectionManager
import com.openlattice.assembler.AssemblerConnectionManagerDependent
import com.openlattice.assembler.EntitySetAssemblyKey
import com.openlattice.assembler.MaterializedEntitySet
import com.openlattice.rhizome.hazelcast.entryprocessors.AbstractReadOnlyRhizomeEntryProcessor
import org.slf4j.LoggerFactory

private val logger = LoggerFactory.getLogger(RenameMaterializedEntitySetProcessor::class.java)

data class RenameMaterializedEntitySetProcessor(val oldName: String, val newName: String)
    : AbstractReadOnlyRhizomeEntryProcessor<EntitySetAssemblyKey, MaterializedEntitySet?, Void?>(),
        AssemblerConnectionManagerDependent<RenameMaterializedEntitySetProcessor>,
        Offloadable {
    @Transient
    private var acm: AssemblerConnectionManager? = null

    override fun process(entry: MutableMap.MutableEntry<EntitySetAssemblyKey, MaterializedEntitySet?>): Void? {
        val organizationId = entry.key.organizationId
        if (entry.value == null) {
            logger.error("Encountered null assembly while trying to rename entity set ${entry.key.entitySetId} " +
                    "materialized view in organization $organizationId.")
        } else {
            acm?.renameMaterializedEntitySet(organizationId, oldName, newName)
                    ?: throw IllegalStateException(AssemblerConnectionManagerDependent.NOT_INITIALIZED)
        }
        return null
    }

    override fun getExecutorName(): String {
        return ExecutionService.OFFLOADABLE_EXECUTOR
    }

    override fun init(acm: AssemblerConnectionManager): RenameMaterializedEntitySetProcessor {
        this.acm = acm
        return this
    }
}