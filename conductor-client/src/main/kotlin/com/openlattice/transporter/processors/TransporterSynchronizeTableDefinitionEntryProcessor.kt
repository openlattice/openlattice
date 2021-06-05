package com.openlattice.transporter.processors

import com.hazelcast.core.Offloadable
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.edm.type.PropertyType
import com.openlattice.transporter.tableDefinition
import com.openlattice.transporter.transportTable
import com.openlattice.transporter.types.TransporterColumnSet
import com.openlattice.transporter.types.TransporterDatastore
import com.openlattice.transporter.types.TransporterDependent
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import org.slf4j.LoggerFactory
import java.util.*

/**
 * Synchronizes property table definitions for [newProperties]
 * A return value of true indicates that columns were added to or removed from the entity type tables
 */
@SuppressFBWarnings(value = ["SE_BAD_FIELD"], justification = "Custom Stream Serializer is implemented")
data class TransporterSynchronizeTableDefinitionEntryProcessor(
        val newProperties: Collection<PropertyType> = setOf(),
        val removedProperties: Collection<PropertyType> = setOf()
): AbstractRhizomeEntryProcessor<UUID, TransporterColumnSet, Boolean>(),
        Offloadable,
        TransporterDependent<TransporterSynchronizeTableDefinitionEntryProcessor>
{
    companion object {
        private val logger = LoggerFactory.getLogger(TransporterSynchronizeTableDefinitionEntryProcessor::class.java)
    }

    @Transient
    private lateinit var data: TransporterDatastore

    override fun process(entry: MutableMap.MutableEntry<UUID, TransporterColumnSet>): Boolean {
        check(::data.isInitialized) { TransporterDependent.NOT_INITIALIZED }
        // transport is a separate function strictly to allow for easier early returns
        return transport(entry)
    }

    private fun transport(entry: MutableMap.MutableEntry<UUID, TransporterColumnSet>): Boolean {
        val toRemove = removedProperties.filter { entry.value.containsKey(it.id) }
        val newProps = newProperties.filter { !entry.value.containsKey(it.id) }
        if (newProps.isEmpty() && toRemove.isEmpty()) {
            return false
        }
        val transporter = data.datastore()
        val removedCols = entry.value.withoutProperties(toRemove).values.map{ it.transporterColumn() }
        val newColumns = entry.value.withAndWithoutProperties(newProps, toRemove)
        val table = tableDefinition(entry.key, newColumns.columns.values.map { it.transporterColumn() })

        transporter.connection.use { conn ->
            transportTable(table, conn, logger, removedCols)
            entry.setValue(newColumns)
        }
        return newColumns.isNotEmpty() || removedCols.isNotEmpty()
    }

    override fun getExecutorName(): String = Offloadable.OFFLOADABLE_EXECUTOR

    override fun init(data: TransporterDatastore): TransporterSynchronizeTableDefinitionEntryProcessor {
        this.data = data
        return this
    }
}