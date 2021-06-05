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
import com.hazelcast.spi.impl.executionservice.ExecutionService
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.assembler.AssemblerConnectionManager
import com.openlattice.assembler.AssemblerConnectionManagerDependent
import com.openlattice.assembler.OrganizationAssembly
import com.openlattice.authorization.SecurablePrincipal
import com.openlattice.edm.EntitySet
import com.openlattice.edm.type.PropertyType
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import java.util.*

@SuppressFBWarnings(value = ["SE_BAD_FIELD"], justification = "Custom Stream Serializer is implemented")
data class AddMembersToOrganizationAssemblyProcessor(
        val authorizedPropertyTypesOfEntitySetsByPrincipal: Map<SecurablePrincipal, Map<EntitySet, Collection<PropertyType>>>
) : AbstractRhizomeEntryProcessor<UUID, OrganizationAssembly, Void?>(false),
        AssemblerConnectionManagerDependent<AddMembersToOrganizationAssemblyProcessor>,
        Offloadable,
        ReadOnly {

    @Transient
    private lateinit var acm: AssemblerConnectionManager

    override fun process(entry: MutableMap.MutableEntry<UUID, OrganizationAssembly?>): Void? {
        val organizationId = entry.key
        val assembly = entry.value
        check(assembly != null) {
            "Encountered null assembly while trying to add new principals " +
                    "${authorizedPropertyTypesOfEntitySetsByPrincipal.keys} to organization $organizationId."
        }

        check(::acm.isInitialized) { AssemblerConnectionManagerDependent.NOT_INITIALIZED }
        acm.addMembersToOrganization(organizationId, authorizedPropertyTypesOfEntitySetsByPrincipal)

        return null
    }

    override fun getExecutorName(): String {
        return ExecutionService.OFFLOADABLE_EXECUTOR
    }

    override fun init(acm: AssemblerConnectionManager): AddMembersToOrganizationAssemblyProcessor {
        this.acm = acm
        return this
    }
}