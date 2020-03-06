package com.openlattice.transporter.processors

import com.hazelcast.core.Offloadable
import com.openlattice.edm.EntitySet
import com.openlattice.postgres.PostgresArrays
import com.openlattice.rhizome.hazelcast.entryprocessors.AbstractReadOnlyRhizomeEntryProcessor
import com.openlattice.transporter.hasModifiedData
import com.openlattice.transporter.tableName
import com.openlattice.transporter.types.TransporterColumnSet
import com.openlattice.transporter.types.TransporterDatastore
import com.openlattice.transporter.types.TransporterDependent
import com.openlattice.transporter.types.transporterNamespace
import com.openlattice.transporter.updateIdsForEntitySets
import com.openlattice.transporter.updateOneBatchForProperty
import io.prometheus.client.Counter
import org.slf4j.LoggerFactory
import java.util.*

class TransporterPropagateDataEntryProcessor(val entitySets: Set<EntitySet>, val partitions: Collection<Int>):
        AbstractReadOnlyRhizomeEntryProcessor<UUID, TransporterColumnSet, Void?>(),
        Offloadable
{
    companion object {
        private val logger = LoggerFactory.getLogger(TransporterPropagateDataEntryProcessor::class.java)
        val idCounter: Counter = Counter.build()
                .namespace(transporterNamespace)
                .name("inserted_ids")
                .help("Rows updated from the ids table in the transporter database")
                .register()
        val valueCounter: Counter = Counter.build()
                .namespace(transporterNamespace)
                .name("updated_values")
                .help("Rows updated in the transporter database")
                .register()
        val errorCounter: Counter = Counter.build()
                .namespace(transporterNamespace)
                .name("errors")
                .help("Errors occurred during copies to the transporter database")
                .register()
    }

    @Transient
    private lateinit var data: TransporterDatastore

    override fun process(entry: Map.Entry<UUID, TransporterColumnSet>): Void? {
        check(::data.isInitialized) { TransporterDependent.NOT_INITIALIZED }
        transport(entry)
        return null
    }

    private fun transport(entry: Map.Entry<UUID, TransporterColumnSet>) {
        if (entitySets.isEmpty() || partitions.isEmpty()) {
            return
        }
        val tableName = tableName(entry.key)
        entitySets.filter { it.isLinking }.forEach {
            // should be a noop because it's always filtered out but just in case...
            logger.error("Skipping linking entity set {} ({})", it.name, it.id)
        }
        val entitySetIds = entitySets
                .filterNot { it.isLinking }
                .map { it.id }
                .toSet()
        val transporter = data.datastore()
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
                    lastSql = updateOneBatchForProperty(tableName, id, col)
                    conn.prepareStatement(lastSql).use { ps ->
                        ps.setArray(1, partitions)
                        ps.setArray(2, entitySetArray)
                        generateSequence { ps.executeUpdate() }.takeWhile { it > 0 }.sum()
                    }
                }.sum()
                logger.debug("Updated {} values in entity type table {}", colUpdates, tableName)
                conn.commit()
                idCounter.inc(idUpdates.toDouble())
                valueCounter.inc((colUpdates).toDouble())
            } catch (ex: Exception) {
                errorCounter.inc()
                logger.error("Unable to update transporter: SQL: {}", lastSql, ex)
                conn.rollback()
                throw ex
            }
        }
    }

    override fun getExecutorName(): String {
        return Offloadable.OFFLOADABLE_EXECUTOR
    }

    fun init(data: TransporterDatastore): TransporterPropagateDataEntryProcessor {
        this.data = data
        return this
    }
}