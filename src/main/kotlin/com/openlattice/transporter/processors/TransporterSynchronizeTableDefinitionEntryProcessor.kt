package com.openlattice.transporter.processors

import com.hazelcast.core.Offloadable
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.edm.type.PropertyType
import com.openlattice.transporter.tableDefinition
import com.openlattice.transporter.transportTable
import com.openlattice.transporter.types.TransporterColumnSet
import com.openlattice.transporter.types.TransporterDatastore
import com.openlattice.transporter.types.TransporterDependent
import org.slf4j.LoggerFactory
import java.util.*

/**
 * Synchronizes property table definitions for [newProperties]
 */
data class TransporterSynchronizeTableDefinitionEntryProcessor(val newProperties: Collection<PropertyType>):
        AbstractRhizomeEntryProcessor<UUID, TransporterColumnSet, Void?>(),
        Offloadable
{
    companion object {
        private val logger = LoggerFactory.getLogger(TransporterSynchronizeTableDefinitionEntryProcessor::class.java)
    }

    @Transient
    private lateinit var data: TransporterDatastore

    override fun process(entry: MutableMap.MutableEntry<UUID, TransporterColumnSet>): Void? {
        check(::data.isInitialized) { TransporterDependent.NOT_INITIALIZED }
        // transport is a separate function strictly to allow for easier early returns
        transport(entry)
        return null
    }

    private fun transport(entry: MutableMap.MutableEntry<UUID, TransporterColumnSet>) {
        val newProps = newProperties.filter { !entry.value.containsKey(it.id) }
        if (newProps.isEmpty()) {
            return
        }
        /**
         * TODO: drop and create new view when columns added
         */
        val transporter = data.datastore()
        val newColumns = entry.value.withProperties(newProps)
        val table = tableDefinition(entry.key, newColumns.columns.values.map { it.transporterColumn() })
        transporter.connection.use { conn ->
            transportTable(table, conn, logger)
            entry.setValue(newColumns)
        }
        return
    }

    override fun getExecutorName(): String = Offloadable.OFFLOADABLE_EXECUTOR

    fun init(data: TransporterDatastore): TransporterSynchronizeTableDefinitionEntryProcessor {
        this.data = data
        return this
    }
}