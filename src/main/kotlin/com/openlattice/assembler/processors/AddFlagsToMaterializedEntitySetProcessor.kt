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
import com.openlattice.assembler.EntitySetAssemblyKey
import com.openlattice.assembler.MaterializedEntitySet
import com.openlattice.organization.OrganizationEntitySetFlag

data class AddFlagsToMaterializedEntitySetProcessor(val flags: Set<OrganizationEntitySetFlag>)
    : AbstractRhizomeEntryProcessor<EntitySetAssemblyKey, MaterializedEntitySet, MaterializedEntitySet>() {

    override fun process(entry: MutableMap.MutableEntry<EntitySetAssemblyKey, MaterializedEntitySet>?)
            : MaterializedEntitySet {
        val materializedEntitySet = entry!!.value
        val flagsHaveBeenAdded = materializedEntitySet.flags.addAll(flags)
        if (flagsHaveBeenAdded) {
            entry.setValue(materializedEntitySet)
        }

        return materializedEntitySet
    }
}