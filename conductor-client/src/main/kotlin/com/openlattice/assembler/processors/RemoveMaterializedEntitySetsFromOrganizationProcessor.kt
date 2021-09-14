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

import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.assembler.OrganizationAssembly
import java.lang.IllegalStateException
import java.util.UUID

data class RemoveMaterializedEntitySetsFromOrganizationProcessor(val entitySetIds: List<UUID>)
    : AbstractRhizomeEntryProcessor<UUID, OrganizationAssembly, Void?>() {

    override fun process(entry: MutableMap.MutableEntry<UUID, OrganizationAssembly?>): Void? {
        val assembly = entry.value
        if (assembly == null) {
            throw IllegalStateException("Encountered null assembly while trying to remove materialized entity sets " +
                    "from organization ${entry.key}.")
        } else {
            assembly.materializedEntitySets.minusAssign(entitySetIds)
            entry.setValue(assembly)
        }

        return null
    }
}