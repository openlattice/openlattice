package com.openlattice.assembler.processors

import com.hazelcast.core.Offloadable
import com.hazelcast.spi.ExecutionService
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.assembler.AssemblerConnectionManager
import com.openlattice.assembler.OrganizationAssembly
import com.openlattice.assembler.PRODUCTION_FOREIGN_SCHEMA
import org.slf4j.LoggerFactory
import java.lang.IllegalStateException
import java.util.UUID


private val logger = LoggerFactory.getLogger(CreateProductionForeignTableOfEntitySetProcessor::class.java)
private const val NOT_INITIALIZED = "Assembler Connection Manager not initialized."

data class CreateProductionForeignTableOfEntitySetProcessor(val entitySetId: UUID) :
        AbstractRhizomeEntryProcessor<UUID, OrganizationAssembly, Void?>(true), Offloadable {
    @Transient
    private var acm: AssemblerConnectionManager? = null

    override fun process(entry: MutableMap.MutableEntry<UUID, OrganizationAssembly?>): Void? {
        val organizationId = entry.key
        val assembly = entry.value
        if (assembly == null) {
            logger.error("Encountered null assembly while trying to create $PRODUCTION_FOREIGN_SCHEMA foreign table " +
                    "for entity set $entitySetId.")
        } else {
            acm?.createProductionForeignSchemaOfEntitySetIfNotExists(organizationId, entitySetId)
                    ?: throw IllegalStateException(NOT_INITIALIZED)
        }

        return null
    }

    override fun getExecutorName(): String {
        return ExecutionService.OFFLOADABLE_EXECUTOR
    }

    fun init(acm: AssemblerConnectionManager): CreateProductionForeignTableOfEntitySetProcessor {
        this.acm = acm
        return this
    }

}