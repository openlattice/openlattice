package com.openlattice.transporter.processors

import com.hazelcast.core.Offloadable
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.assembler.AssemblerConnectionManager
import com.openlattice.assembler.AssemblerConnectionManagerDependent
import com.openlattice.edm.type.PropertyType
import com.openlattice.transporter.tableDefinition
import com.openlattice.transporter.types.TransporterColumnSet
import org.slf4j.LoggerFactory
import java.util.*

data class TransporterSynchronizeTableDefinitionEntryProcessor(val newProperties: Collection<PropertyType>):
        AbstractRhizomeEntryProcessor<UUID, TransporterColumnSet, Void?>(),
        Offloadable,
        AssemblerConnectionManagerDependent<TransporterSynchronizeTableDefinitionEntryProcessor> {
    companion object {
        private val logger = LoggerFactory.getLogger(TransporterSynchronizeTableDefinitionEntryProcessor::class.java)
    }

    private lateinit var acm: AssemblerConnectionManager

    override fun process(entry: MutableMap.MutableEntry<UUID, TransporterColumnSet>): Void? {
        transport(entry)
        return null
    }

    private fun transport(entry: MutableMap.MutableEntry<UUID, TransporterColumnSet>) {
        val transporter = acm.connect("transporter")
        val newProps = newProperties.filter { !entry.value.containsKey(it.id) }
        if (newProps.isEmpty()) {
            return
        }
        val newColumns = entry.value.withProperties(newProps)
        val table = tableDefinition(entry.key, newColumns.columns.values.map { it.destCol() })
        transporter.connection.use { conn ->
            var lastSql = ""
            try {
                conn.createStatement().use { st ->
                    lastSql = table.createTableQuery()
                    st.execute(lastSql)
                    lastSql = "SELECT * FROM ${table.name} where 1 = 0"
                    val currentPropertyIds = st.executeQuery(lastSql).use { rs ->
                        val rsmd = rs.metaData
                        (1..rsmd.columnCount)
                                .map {i -> rsmd.getColumnName(i) }
                                .toSet()
                    }
                    val newCols = newColumns.columns
                            .filterKeys { !currentPropertyIds.contains(it.toString()) }
                            .values
                            .map { it.destCol() }
                    if (newCols.isNotEmpty()) {
                        lastSql = "ALTER TABLE ${table.name} " +
                                newCols.joinToString(", ") {
                                    col -> " ADD COLUMN ${col.name} ${col.datatype.sql()}"
                                }
                        st.execute(lastSql)
                    }
                }
                entry.setValue(newColumns)
            } catch (e: Exception) {
                logger.error("Unable to execute query: {}", lastSql, e)
                conn.rollback()
                throw e
            }
        }
        return
    }

    override fun getExecutorName(): String = Offloadable.OFFLOADABLE_EXECUTOR

    override fun init(acm: AssemblerConnectionManager): TransporterSynchronizeTableDefinitionEntryProcessor {
        this.acm = acm
        return this
    }

}