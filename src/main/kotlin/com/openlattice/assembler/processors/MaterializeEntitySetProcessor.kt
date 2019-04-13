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
import com.openlattice.assembler.EntitySetAssemblyKey
import com.openlattice.assembler.MaterializedEntitySet
import com.openlattice.edm.type.PropertyType
import org.slf4j.LoggerFactory
import java.lang.IllegalStateException
import java.util.*


private val logger = LoggerFactory.getLogger(MaterializeEntitySetProcessor::class.java)
private const val NOT_INITIALIZED = "Assembler Connection Manager not initialized."

/**
 * An offloadable entity processor that materializes an entity set.
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */

data class MaterializeEntitySetProcessor(val authorizedPropertyTypes: Map<UUID, PropertyType>
) : AbstractRhizomeEntryProcessor<EntitySetAssemblyKey, MaterializedEntitySet, Void?>(true), Offloadable {
    @Transient
    private var acm: AssemblerConnectionManager? = null

    override fun process(entry: MutableMap.MutableEntry<EntitySetAssemblyKey, MaterializedEntitySet?>): Void? {
        val entitySetAssemblyKey = entry.key
        val materializedEntitySet = entry.value
        if (materializedEntitySet == null) {
            logger.error("Materialized entity set with id {} for organization id {} was not initialized properly.")
        } else {
            acm?.materializeEntitySets(
                    entitySetAssemblyKey.organizationId,
                    mapOf(entitySetAssemblyKey.entitySetId to authorizedPropertyTypes)
            ) ?: throw IllegalStateException(NOT_INITIALIZED)
        }

        return null
    }

    fun init(acm: AssemblerConnectionManager): MaterializeEntitySetProcessor {
        this.acm = acm
        return this
    }

    override fun getExecutorName(): String {
        return ExecutionService.OFFLOADABLE_EXECUTOR
    }
}