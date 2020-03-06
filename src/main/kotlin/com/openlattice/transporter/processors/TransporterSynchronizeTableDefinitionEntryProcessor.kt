package com.openlattice.transporter.processors

import com.hazelcast.core.Offloadable
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.edm.type.PropertyType
import com.openlattice.transporter.addAllMissingColumnsQuery
import com.openlattice.transporter.tableDefinition
import com.openlattice.transporter.types.TransporterColumnSet
import com.openlattice.transporter.types.TransporterDatastore
import com.openlattice.transporter.types.TransporterDependent
import org.slf4j.LoggerFactory
import java.util.*

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
        val transporter = data.datastore()
        val newProps = newProperties.filter { !entry.value.containsKey(it.id) }
        if (newProps.isEmpty()) {
            return
        }
        val newColumns = entry.value.withProperties(newProps)
        val table = tableDefinition(entry.key, newColumns.columns.values.map { it.transporterColumn() })
        transporter.connection.use { conn ->
            var lastSql = ""
            try {
                conn.createStatement().use { st ->
                    lastSql = table.createTableQuery()
                    st.execute(lastSql)
                    lastSql = addAllMissingColumnsQuery(table)
                    st.execute(lastSql)
                    table.createIndexQueries.forEach {
                        lastSql = it
                        st.execute(it)
                    }
                }
                entry.setValue(newColumns)
            } catch (e: Exception) {
                logger.error("Unable to execute query: {}", lastSql, e)
                throw e
            }
        }
        return
    }

    override fun getExecutorName(): String = Offloadable.OFFLOADABLE_EXECUTOR

    fun init(data: TransporterDatastore): TransporterSynchronizeTableDefinitionEntryProcessor {
        this.data = data
        return this
    }
}