package com.openlattice.transporter.processors

import com.hazelcast.core.Offloadable
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.assembler.AssemblerConnectionManager
import com.openlattice.assembler.AssemblerConnectionManagerDependent
import com.openlattice.edm.type.PropertyType
import com.openlattice.transporter.tableDefinition
import com.openlattice.transporter.types.TransporterColumnSet
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.util.*

data class TransporterSynchronizeTableDefinitionEntryProcessor(val newProperties: Collection<PropertyType>):
        AbstractRhizomeEntryProcessor<UUID, TransporterColumnSet, Unit>(),
        Offloadable,
        AssemblerConnectionManagerDependent<TransporterSynchronizeTableDefinitionEntryProcessor> {
    companion object {
        private val logger = LoggerFactory.getLogger(TransporterSynchronizeTableDefinitionEntryProcessor::class.java)
    }

    private lateinit var acm: AssemblerConnectionManager

    override fun process(entry: MutableMap.MutableEntry<UUID, TransporterColumnSet>) {
        val transporter = acm.connect("transporter")
        val newProps = newProperties.filter { entry.value.containsKey(it.id) }
        if (newProps.isEmpty()) {
            return
        }
        val newEntry = entry.value.withProperties(newProps)
        val table = tableDefinition(entry.key, newEntry.columns.values.map { it.destCol() })
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
                    val newCols = newEntry.columns
                            .filterKeys { currentPropertyIds.contains(it.toString()) }
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
                entry.setValue(newEntry)
            } catch (e: Exception) {
                logger.error("Unable to execute query: {}", lastSql, e)
                conn.rollback()
                throw e
            }
        }
    }

    override fun getExecutorName(): String = Offloadable.OFFLOADABLE_EXECUTOR

    override fun init(acm: AssemblerConnectionManager): TransporterSynchronizeTableDefinitionEntryProcessor {
        this.acm = acm
        return this
    }

}