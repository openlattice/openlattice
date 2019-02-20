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
import com.openlattice.edm.type.PropertyType
import org.slf4j.LoggerFactory
import java.lang.IllegalStateException
import java.util.*


private val logger = LoggerFactory.getLogger(MaterializeEntitySetsProcessor::class.java)
private const val NOT_INITIALIZED = "Assembler Connection Manager not initialized."

/**
 * An offloadable entity processor that materializes an entity set.
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */

data class MaterializeEntitySetsProcessor(
        val authorizedPropertyTypesByEntitySet: Map<UUID, Map<UUID, PropertyType>>
) : AbstractRhizomeEntryProcessor<UUID, OrganizationAssembly, Void?>(true), Offloadable {
    @Transient
    private var acm: AssemblerConnectionManager? = null

    val entitySetIds: Set<UUID> = authorizedPropertyTypesByEntitySet.keys
    override fun process(entry: MutableMap.MutableEntry<UUID, OrganizationAssembly?>): Void? {
        val organizationId = entry.key
        val assembly = entry.value
        if (assembly == null) {
            logger.error("Encountered null assembly while trying to materialize entity sets.")
        } else {
            acm?.materializeEntitySets(organizationId, authorizedPropertyTypesByEntitySet)
                    ?: throw IllegalStateException(NOT_INITIALIZED)
            assembly.entitySetIds.addAll(entitySetIds)
            entry.setValue(assembly)
        }

        return null
    }

    fun init(acm: AssemblerConnectionManager): MaterializeEntitySetsProcessor {
        this.acm = acm
        return this
    }

    override fun getExecutorName(): String {
        return ExecutionService.OFFLOADABLE_EXECUTOR
    }
}