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
import com.openlattice.assembler.OrganizationAssembly
import com.openlattice.assembler.PostgresDatabases
import com.openlattice.organizations.PrincipalSet
import java.lang.IllegalStateException
import java.util.*

data class AddMembersToOrganizationAssemblyProcessor(val newMembers: PrincipalSet)
    : AbstractRhizomeEntryProcessor<UUID, OrganizationAssembly, Void?>(), Offloadable {

    @Transient
    private lateinit var acm: AssemblerConnectionManager

    override fun process(entry: MutableMap.MutableEntry<UUID, OrganizationAssembly?>): Void? {
        val organizationId = entry.key
        val assembly = entry.value
        if (assembly == null) {
            throw IllegalStateException("Encountered null assembly while trying to add new members $newMembers to " +
                    "organization $organizationId.")
        } else {
            val dbName = PostgresDatabases.buildOrganizationDatabaseName(organizationId)
            acm.connect(dbName).use { dataSource -> acm.addMembersToOrganization(dbName, dataSource, newMembers) }
        }

        return null
    }

    override fun getExecutorName(): String {
        return ExecutionService.OFFLOADABLE_EXECUTOR
    }

    fun init(acm: AssemblerConnectionManager): AddMembersToOrganizationAssemblyProcessor {
        this.acm = acm
        return this
    }

}