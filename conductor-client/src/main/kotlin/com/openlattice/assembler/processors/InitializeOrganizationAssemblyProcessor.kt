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
import com.openlattice.assembler.OrganizationAssembly
import org.slf4j.LoggerFactory
import java.util.*

private val logger = LoggerFactory.getLogger(InitializeOrganizationAssemblyProcessor::class.java)

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */

class InitializeOrganizationAssemblyProcessor( val dbName: String ) :
        AbstractRhizomeEntryProcessor<UUID, OrganizationAssembly, Void?>(),
        AssemblerConnectionManagerDependent<InitializeOrganizationAssemblyProcessor>,
        Offloadable {
    @Transient
    private var acm: AssemblerConnectionManager? = null

    override fun process(entry: MutableMap.MutableEntry<UUID, OrganizationAssembly?>): Void? {
        val organizationId = entry.key
        val assembly = entry.value
        when {
            assembly == null -> {
                logger.error("Assembly for organization id {} was not initialized properly.", organizationId)
            }
            assembly.initialized -> logger.info(
                    "The database for organization {} has already been initialized",
                    organizationId
            )
            else -> {
                acm?.createAndInitializeOrganizationDatabase(organizationId, dbName)
                        ?: throw IllegalStateException(AssemblerConnectionManagerDependent.NOT_INITIALIZED)
                assembly.initialized = true
                entry.setValue(assembly)
            }
        }
        return null
    }

    override fun init(acm: AssemblerConnectionManager): InitializeOrganizationAssemblyProcessor {
        this.acm = acm
        return this
    }

    override fun getExecutorName(): String {
        return ExecutionService.OFFLOADABLE_EXECUTOR
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as InitializeOrganizationAssemblyProcessor

        if (dbName != other.dbName) return false

        return true
    }

    override fun hashCode(): Int {
        return dbName.hashCode()
    }
}
