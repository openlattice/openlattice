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
import com.hazelcast.core.ReadOnly
import com.hazelcast.spi.ExecutionService
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.assembler.AssemblerConnectionManager
import com.openlattice.assembler.AssemblerConnectionManagerDependent
import com.openlattice.assembler.OrganizationAssembly
import com.openlattice.authorization.Principal
import java.util.UUID


data class MaterializeEdgesProcessor(val authorizedPrincipals: Set<Principal>)
    : AbstractRhizomeEntryProcessor<UUID, OrganizationAssembly, Void?>(false),
        AssemblerConnectionManagerDependent<MaterializeEdgesProcessor>,
        Offloadable,
        ReadOnly {
    @Transient
    private var acm: AssemblerConnectionManager? = null

    override fun process(entry: MutableMap.MutableEntry<UUID, OrganizationAssembly?>): Void? {
        val organizationId = entry.key
        val assembly = entry.value
        if (assembly == null) {
            throw IllegalStateException("Encountered null assembly while trying to materialize edges for " +
                    "organization $organizationId.")
        } else {
            acm?.materializeEdges(organizationId, assembly.materializedEntitySets.keys, authorizedPrincipals)
                    ?: throw IllegalStateException(AssemblerConnectionManagerDependent.NOT_INITIALIZED)
        }

        return null
    }

    override fun getExecutorName(): String {
        return ExecutionService.OFFLOADABLE_EXECUTOR
    }

    override fun init(acm: AssemblerConnectionManager): MaterializeEdgesProcessor {
        this.acm = acm
        return this
    }
}