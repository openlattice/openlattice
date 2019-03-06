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
import com.openlattice.assembler.PRODUCTION_FOREIGN_SCHEMA
import com.openlattice.edm.type.PropertyType
import org.slf4j.LoggerFactory
import java.lang.IllegalStateException
import java.util.UUID

private val logger = LoggerFactory.getLogger(UpdateProductionForeignTableOfEntitySetProcessor::class.java)
private const val NOT_INITIALIZED = "Assembler Connection Manager not initialized."

data class UpdateProductionForeignTableOfEntitySetProcessor(
        val entitySetId: UUID,
        val newPropertyTypes: List<PropertyType>) :
        AbstractRhizomeEntryProcessor<UUID, OrganizationAssembly, Void?>(), Offloadable {
    @Transient
    private var acm: AssemblerConnectionManager? = null

    override fun process(entry: MutableMap.MutableEntry<UUID, OrganizationAssembly?>): Void? {
        val organizationId = entry.key
        val assembly = entry.value
        if (assembly == null) {
            logger.error("Encountered null assembly while trying to update $PRODUCTION_FOREIGN_SCHEMA foreign table " +
                    "for entity set $entitySetId with new properties $newPropertyTypes.")
        } else {
            acm?.updateProductionForeignSchemaOfEntitySet(organizationId, entitySetId, newPropertyTypes)
                    ?: throw IllegalStateException(NOT_INITIALIZED)
        }

        return null
    }

    override fun getExecutorName(): String {
        return ExecutionService.OFFLOADABLE_EXECUTOR
    }

    fun init(acm: AssemblerConnectionManager): UpdateProductionForeignTableOfEntitySetProcessor {
        this.acm = acm
        return this
    }

}