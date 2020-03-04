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
import com.openlattice.transporter.updateIdsForEntitySets
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
        val id_counter: Counter = Counter.build()
                .namespace("transporter_data")
                .name("inserted_ids")
                .help("Rows updated from the ids table in the transporter database")
                .register()
        val value_counter: Counter = Counter.build()
                .namespace("transporter_data")
                .name("updated_values")
                .help("Rows updated in the transporter database")
                .register()
        val error_counter: Counter = Counter.build()
                .namespace("transporter_data")
                .name("errors")
                .help("Errors occurred during copies to the transporter database")
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
        val entitySetIds = entitySets.map { it.id }.toSet()
        val transporter = acm.connect("transporter")
        entitySets.filter {
            if (it.isLinking) {
                logger.error("Skipping linking entity set {} ({})", it.name, it.id)
            }
            !it.isLinking
        }
        if (!hasModifiedData(transporter, partitions, entitySetIds)) {
            return
        }
        transporter.connection.use { conn ->
            conn.autoCommit = false
            var lastSql = ""
            try {
                val entitySetArray = PostgresArrays.createUuidArray(conn, entitySetIds)
                val partitions = PostgresArrays.createIntArray(conn, partitions)
                lastSql = updateIdsForEntitySets(tableName)
                val idUpdates = conn.prepareStatement(lastSql).use {ps ->
                    ps.setArray(1, partitions)
                    ps.setArray(2, entitySetArray)
                    ps.executeUpdate()
                }
                val colUpdates = entry.value.map { (id, col) ->
                    val query = updateOneBatchForProperty(tableName, id, col)
                    lastSql = query
                    conn.prepareStatement(query).use { ps ->
                        ps.setArray(1, partitions)
                        ps.setArray(2, entitySetArray)
                        generateSequence { ps.executeUpdate() }.takeWhile { it > 0 }.sum()
                    }
                }.sum()
                logger.debug("Updated {} values in entity type table {}", colUpdates, tableName)
                conn.commit()
                id_counter.inc(idUpdates.toDouble())
                value_counter.inc((colUpdates).toDouble())
            } catch (ex: Exception) {
                error_counter.inc()
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