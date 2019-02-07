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

import com.hazelcast.core.HazelcastInstance
import com.hazelcast.core.HazelcastInstanceAware
import com.hazelcast.core.Offloadable
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.assembler.AssemblerConnectionManager
import com.openlattice.assembler.AssemblerConnectionManager.Companion.createDatabase
import com.openlattice.assembler.OrganizationAssembly
import com.openlattice.assembler.createRoleIfNotExistsSql
import com.openlattice.assembler.createUserIfNotExistsSql
import com.openlattice.postgres.DataTables
import org.slf4j.LoggerFactory
import java.util.*

private val logger = LoggerFactory.getLogger(InitializeOrganizationAssemblyProcessor::class.java)

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */

class InitializeOrganizationAssemblyProcessor : AbstractRhizomeEntryProcessor<UUID, OrganizationAssembly, Void?>(), Offloadable {
    override fun process(entry: MutableMap.MutableEntry<UUID, OrganizationAssembly?>): Void? {
        val organizationId = entry.key
        val assembly = entry.value
        when {
            assembly == null -> {
                logger.error("Assembly for organization id {} was not initialized properly.")
            }
            assembly.initialized -> logger.info(
                    "The database with name {} for organization {} has already been initialized",
                    organizationId,
                    assembly.dbname
            )
            else -> {
                AssemblerConnectionManager.createOrganizationDatabase(organizationId)
                assembly.initialized = true
                entry.setValue(assembly)
            }
        }
        return null
    }

    override fun getExecutorName(): String {
        return "default"
    }


}
