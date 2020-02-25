package com.openlattice.transporter.processors

import com.hazelcast.core.Offloadable
import com.openlattice.assembler.AssemblerConnectionManager
import com.openlattice.assembler.AssemblerConnectionManagerDependent
import com.openlattice.edm.EntitySet
import com.openlattice.postgres.PostgresArrays
import com.openlattice.rhizome.hazelcast.entryprocessors.AbstractReadOnlyRhizomeEntryProcessor
import com.openlattice.transporter.hasModifiedData
import com.openlattice.transporter.tableName
import com.openlattice.transporter.types.TransporterColumnSet
import com.openlattice.transporter.updateOneBatchForProperty
import io.prometheus.client.Counter
import org.slf4j.LoggerFactory
import java.util.*

class TransporterPropagateDataEntryProcessor(val entitySets: Set<EntitySet>, val partitions: Collection<Int>):
        AbstractReadOnlyRhizomeEntryProcessor<UUID, TransporterColumnSet, Void?>(),
        Offloadable,
        AssemblerConnectionManagerDependent<TransporterPropagateDataEntryProcessor>
{
    companion object {
        private val logger = LoggerFactory.getLogger(TransporterPropagateDataEntryProcessor::class.java)
        val counter: Counter = Counter.build()
                .namespace("transporter")
                .name("rows_updated")
                .help("Rows updated in the transporter database")
                .register()
    }

    @Transient
    private lateinit var acm: AssemblerConnectionManager

    override fun process(entry: Map.Entry<UUID, TransporterColumnSet>): Void? {
        transport(entry)
        return null
    }

    private fun transport(entry: Map.Entry<UUID, TransporterColumnSet>) {
        if (entitySets.isEmpty() || partitions.isEmpty()) {
            return
        }
        val tableName = tableName(entry.key)
        val propertyIds = entry.value.columns.keys
        val entitySetIds = entitySets.map { it.id }.toSet()
        val transporter = acm.connect("transporter")
        entitySets.filter {
            if (it.isLinking) {
                logger.error("Skipping linking entity set {} ({})", it.name, it.id)
            }
            !it.isLinking
        }
        if (!hasModifiedData(transporter, entitySetIds, propertyIds, partitions)) {
            return
        }
        transporter.connection.use { conn ->
            conn.autoCommit = false
            var lastSql = ""
            try {
                val entitySetArray = PostgresArrays.createUuidArray(conn, entitySetIds)
                val partitions = PostgresArrays.createIntArray(conn, partitions)
                val updates = entry.value.map { propCol ->
                    val query = updateOneBatchForProperty(tableName, propCol)
                    lastSql = query
                    conn.prepareStatement(query).use { ps ->
                        ps.setArray(1, partitions)
                        ps.setArray(2, entitySetArray)
                        generateSequence { ps.executeUpdate() }.takeWhile { it > 0 }.sum()
                    }
                }.sum()
                counter.inc(updates.toDouble())
                logger.debug("Updated {} values in entity type table {}", updates, tableName)
                conn.commit()
            } catch (ex: Exception) {
                logger.error("Unable to update transporter: SQL: {}", lastSql, ex)
                conn.rollback()
                throw ex
            }
        }
    }

    override fun getExecutorName(): String {
        return Offloadable.OFFLOADABLE_EXECUTOR
    }

    override fun init(acm: AssemblerConnectionManager): TransporterPropagateDataEntryProcessor {
        this.acm = acm
        return this
    }
}