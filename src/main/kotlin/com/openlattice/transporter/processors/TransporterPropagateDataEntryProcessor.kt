package com.openlattice.transporter.processors

import com.hazelcast.core.Offloadable
import com.openlattice.edm.EntitySet
import com.openlattice.edm.set.EntitySetFlag
import com.openlattice.postgres.PostgresArrays
import com.openlattice.postgres.ResultSetAdapters
import com.openlattice.postgres.streams.BasePostgresIterable
import com.openlattice.postgres.streams.PreparedStatementHolderSupplier
import com.openlattice.rhizome.hazelcast.entryprocessors.AbstractReadOnlyRhizomeEntryProcessor
import com.openlattice.transporter.hasModifiedData
import com.openlattice.transporter.tableName
import com.openlattice.transporter.transporterNamespace
import com.openlattice.transporter.types.TransporterColumnSet
import com.openlattice.transporter.types.TransporterDatastore
import com.openlattice.transporter.types.TransporterDependent
import com.openlattice.transporter.updateIdsForEntitySets
import com.openlattice.transporter.updateOneBatchForProperty
import com.openlattice.transporter.updatePrimaryKeyForEntitySets
import io.prometheus.client.Counter
import org.slf4j.LoggerFactory
import java.util.*

/**
 * Transport data from enterprise into entity_type tables on atlas
 */
class TransporterPropagateDataEntryProcessor(
        val entitySets: Set<EntitySet>,
        val partitions: Collection<Int>
): AbstractReadOnlyRhizomeEntryProcessor<UUID, TransporterColumnSet, Void?>(),
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
        val edgesCounter: Counter = Counter.build()
                .namespace(transporterNamespace)
                .name("updated_edges")
                .help("Rows updated in the transporter edges table")
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
                .filter{ !it.isLinking && it.flags.contains(EntitySetFlag.MATERIALIZED) }
                .map { it.id }
                .toSet()
        val transporter = data.datastore()
        if (!hasModifiedData(transporter, partitions, entitySetIds)) {
            return
        }


        transporter.connection.use { conn ->
            var lastSql = ""
            try {
                val partitions = PostgresArrays.createIntArray(conn, partitions)
                val entitySetArray = PostgresArrays.createUuidArray(conn, entitySetIds)
                lastSql = updatePrimaryKeyForEntitySets(tableName)

                val idsToLastWrites = BasePostgresIterable(PreparedStatementHolderSupplier(transporter, lastSql ) {
                    it.setArray(1, partitions)
                    it.setArray(2, entitySetArray)
                }) {
                    ResultSetAdapters.id(it) to ResultSetAdapters.lastWriteTyped(it)
                }

                val colUpdates = entry.value.map { (ptId, col) ->
                    val ptBatch = conn.prepareStatement(updateOneBatchForProperty(tableName, ptId, col))
//                    val edgeBatch = conn.prepareStatement(updateOneBatchForEdges(tableName, ptId, col))
                    val idsCommit = conn.prepareStatement(updateIdsForEntitySets())

                    idsToLastWrites.asSequence().map {
                        ptBatch.setArray(1, partitions)
                        ptBatch.setArray(2, entitySetArray)
                        ptBatch.setObject(3, it.first)
                        ptBatch.setObject(4, it.second)
                        ptBatch.addBatch()

//                        edgeBatch.setArray(1, partitions)
//                        edgeBatch.setArray(2, entitySetArray)
//                        edgeBatch.setObject(3, it.first)
//                        edgeBatch.setObject(4, it.second)
//                        edgeBatch.addBatch()

                        idsCommit.setArray(1, partitions)
                        idsCommit.setArray(2, entitySetArray)
                        idsCommit.setObject(3, it.first)
                        idsCommit.setObject(4, it.second)
                        idsCommit.addBatch()
                    }
                    valueCounter.inc( ptBatch.executeBatch().sum().toDouble())
//                    edgesCounter.inc( edgeBatch.executeBatch().sum().toDouble())
                    idCounter.inc( idsCommit.executeBatch().sum().toDouble())
                }

                logger.debug("Updated {} values in entity type table {}", colUpdates, tableName)
                conn.commit()
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