package com.openlattice.transporter.processors

import com.hazelcast.core.Offloadable
import com.openlattice.edm.EntitySet
import com.openlattice.edm.set.EntitySetFlag
import com.openlattice.postgres.PostgresArrays
import com.openlattice.postgres.ResultSetAdapters
import com.openlattice.postgres.streams.BasePostgresIterable
import com.openlattice.postgres.streams.PreparedStatementHolderSupplier
import com.openlattice.rhizome.hazelcast.entryprocessors.AbstractReadOnlyRhizomeEntryProcessor
import com.openlattice.transporter.*
import com.openlattice.transporter.types.TransporterColumnSet
import com.openlattice.transporter.types.TransporterDatastore
import com.openlattice.transporter.types.TransporterDependent
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
                }.toMap()

                val ekidsArray = PostgresArrays.createUuidArray(conn, idsToLastWrites.keys)

                val colUpdates = entry.value.map { (ptId, col) ->
                    logger.debug("transporting data rows")
                    lastSql = updateRowsForPropertyType(tableName, ptId, col)
                    val pts = conn.prepareStatement( lastSql )
                    pts.setArray(1, partitions)
                    pts.setArray(2, entitySetArray)
                    pts.setArray(3, ekidsArray)
                    valueCounter.inc( pts.executeUpdate().toDouble())

                    logger.debug("transporting edge rows")
                    lastSql = updateRowsForEdges()
                    val edgeBatch = conn.prepareStatement( lastSql )
                    edgeBatch.setArray(1, partitions)
                    edgeBatch.setArray(2, entitySetArray)
                    edgeBatch.setArray(3, ekidsArray)
                    edgeBatch.setArray(4, entitySetArray)
                    edgeBatch.setArray(5, ekidsArray)
                    edgeBatch.setArray(6, entitySetArray)
                    edgeBatch.setArray(7, ekidsArray)
                    edgesCounter.inc( edgeBatch.executeUpdate().toDouble())

                    logger.debug("updating last transport timestamps")
                    lastSql = updateLastWriteForId()
                    val idsCommit = conn.prepareStatement( lastSql )
                    idsToLastWrites.forEach { ( ekid, lastWrite ) ->
                        idsCommit.setObject(1, lastWrite)
                        idsCommit.setArray(2, partitions)
                        idsCommit.setArray(3, entitySetArray)
                        idsCommit.setObject(4, ekid)
                        idsCommit.addBatch()
                    }
                    idCounter.inc( idsCommit.executeBatch().sum().toDouble())
                }

                logger.debug("Updated {} data rows in entity type table {}", valueCounter.get(), tableName)
                logger.debug("Updated {} edge rows for entity type table {}", edgesCounter.get(), tableName)
                logger.debug("Updated {} ids for entity type table {}", idCounter.get(), tableName)
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