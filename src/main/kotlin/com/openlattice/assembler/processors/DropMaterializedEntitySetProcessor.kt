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
import com.hazelcast.spi.ExecutionService
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.assembler.AssemblerConnectionManager
import com.openlattice.assembler.EntitySetAssemblyKey
import com.openlattice.assembler.MaterializedEntitySet

private const val NOT_INITIALIZED = "Assembler Connection Manager not initialized."

class DropMaterializedEntitySetProcessor
    : AbstractRhizomeEntryProcessor<EntitySetAssemblyKey, MaterializedEntitySet, Void?>(), Offloadable {

    @Transient
    private var acm: AssemblerConnectionManager? = null

    override fun process(entry: MutableMap.MutableEntry<EntitySetAssemblyKey, MaterializedEntitySet?>): Void? {
        val entitySetAssemblyKey = entry.key
        val materializedEntitySet = entry.value
        if (materializedEntitySet == null) {
            throw IllegalStateException("Encountered null materialized entity set while trying to drop materialized " +
                    "entity set for entity set ${entitySetAssemblyKey.entitySetId} in organization " +
                    "${entitySetAssemblyKey.organizationId}.")
        } else {
            acm?.dematerializeEntitySets(entitySetAssemblyKey.organizationId, setOf(entitySetAssemblyKey.entitySetId))
                    ?: throw IllegalStateException(NOT_INITIALIZED)
            entry.setValue(null)
        }

        return null
    }

    override fun getExecutorName(): String {
        return ExecutionService.OFFLOADABLE_EXECUTOR
    }

    fun init(acm: AssemblerConnectionManager): DropMaterializedEntitySetProcessor {
        this.acm = acm
        return this
    }

    override fun equals(other: Any?): Boolean {
        return (other != null && other is DropMaterializedEntitySetProcessor)
    }

    override fun hashCode(): Int {
        return super.hashCode()
    }
}