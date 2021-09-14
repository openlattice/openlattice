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
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.assembler.AssemblerConnectionManager
import com.openlattice.assembler.AssemblerConnectionManagerDependent
import com.openlattice.assembler.EntitySetAssemblyKey
import com.openlattice.assembler.MaterializedEntitySet
import com.openlattice.edm.EntitySet
import com.openlattice.edm.type.PropertyType
import com.openlattice.organization.OrganizationEntitySetFlag
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import java.time.OffsetDateTime
import java.util.*

@SuppressFBWarnings(value = ["SE_BAD_FIELD"], justification = "Custom Stream Serializer is implemented")
data class UpdateMaterializedEntitySetProcessor(
        val entitySet: EntitySet, val materializablePropertyTypes: Map<UUID, PropertyType>
) : AbstractRhizomeEntryProcessor<EntitySetAssemblyKey, MaterializedEntitySet, Void?>(),
        AssemblerConnectionManagerDependent<UpdateMaterializedEntitySetProcessor>,
        Offloadable {

    @Transient
    private var acm: AssemblerConnectionManager? = null

    override fun process(entry: MutableMap.MutableEntry<EntitySetAssemblyKey, MaterializedEntitySet?>): Void? {
        val organizationId = entry.key.organizationId
        val materializedEntitySet = entry.value
        materializedEntitySet ?: throw IllegalStateException("Encountered null materialized entity set while trying " +
                "to update materialized view for entity set ${entitySet.id} in organization $organizationId.")


        // Clear data and permission unsync flag
        materializedEntitySet.flags.removeAll(
                listOf(
                        OrganizationEntitySetFlag.DATA_UNSYNCHRONIZED,
                        OrganizationEntitySetFlag.MATERIALIZE_PERMISSION_UNSYNCHRONIZED
                )
        )

        // Update last refresh
        materializedEntitySet.lastRefresh = OffsetDateTime.now()

        entry.setValue(materializedEntitySet)

        return null
    }

    override fun init(acm: AssemblerConnectionManager): UpdateMaterializedEntitySetProcessor {
        this.acm = acm
        return this
    }

    override fun getExecutorName(): String {
        return ExecutionService.OFFLOADABLE_EXECUTOR
    }


}