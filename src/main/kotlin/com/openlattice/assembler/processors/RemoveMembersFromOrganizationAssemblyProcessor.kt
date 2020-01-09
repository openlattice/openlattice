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
import com.openlattice.assembler.AssemblerConnectionManager
import com.openlattice.assembler.AssemblerConnectionManagerDependent
import com.openlattice.assembler.OrganizationAssembly
import com.openlattice.assembler.PostgresDatabases
import com.openlattice.authorization.SecurablePrincipal
import com.openlattice.rhizome.hazelcast.entryprocessors.AbstractReadOnlyRhizomeEntryProcessor
import java.util.*

data class RemoveMembersFromOrganizationAssemblyProcessor(val principals: Collection<SecurablePrincipal>)
    : AbstractReadOnlyRhizomeEntryProcessor<UUID, OrganizationAssembly, Void?>(),
        AssemblerConnectionManagerDependent<RemoveMembersFromOrganizationAssemblyProcessor>,
        Offloadable {

    @Transient
    private lateinit var acm: AssemblerConnectionManager

    override fun process(entry: MutableMap.MutableEntry<UUID, OrganizationAssembly?>): Void? {
        val organizationId = entry.key
        val assembly = entry.value
        check(assembly != null) {
            "Encountered null assembly while trying to remove principals $principals from organization $organizationId."
        }

        check(::acm.isInitialized) { AssemblerConnectionManagerDependent.NOT_INITIALIZED }
        val dbName = PostgresDatabases.buildOrganizationDatabaseName(organizationId)
        acm.connect(dbName).let { dataSource -> acm.removeMembersFromOrganization(dbName, dataSource, principals) }

        return null
    }

    override fun getExecutorName(): String {
        return ExecutionService.OFFLOADABLE_EXECUTOR
    }

    override fun init(acm: AssemblerConnectionManager): RemoveMembersFromOrganizationAssemblyProcessor {
        this.acm = acm
        return this
    }
}