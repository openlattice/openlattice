package com.openlattice.transporter.processors

import com.openlattice.edm.EntitySet

data class TransporterPropagateDataEntryProcessor(
    val entitySets: Set<EntitySet>,
    val entitySetPartitions: Collection<Int>
)

/**
 * Transport data from enterprise into entity_type tables on atlas
 */
/*
@SuppressFBWarnings(value = ["SE_BAD_FIELD"], justification = "Custom Stream Serializer is implemented")
class TransporterPropagateDataEntryProcessor(
        val entitySets: Set<EntitySet>,
        val entitySetPartitions: Collection<Int>
): AbstractReadOnlyRhizomeEntryProcessor<UUID, TransporterColumnSet, Void?>(),
        Offloadable,
        TransporterDependent<TransporterPropagateDataEntryProcessor>
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
        val tableName = quotedEtTableName(entry.key)
        entitySets.filter { it.isLinking }.forEach {
            // should be a noop because it's always filtered out but just in case...
            logger.error("Skipping linking entity set {} ({})", it.name, it.id)
        }
        val entitySetIds = entitySets
                .filter{ !it.isLinking && it.flags.contains(EntitySetFlag.TRANSPORTED) }
                .map { it.id }
                .toSet()
        if (entitySets.isEmpty() || entitySetPartitions.isEmpty()) {
            return
        }
        val transporter = data.datastore()
        if (!hasModifiedData(transporter, entitySetPartitions, entitySetIds)) {
            return
        }

        transporter.connection.use { conn ->
            var lastSql = ""
            try {
                val partitions = PostgresArrays.createIntArray(conn, entitySetPartitions)
                val entitySetArray = PostgresArrays.createUuidArray(conn, entitySetIds)

                lastSql = updateEntityTypeTableEntries(tableName)
                val idsToVersions = BasePostgresIterable(PreparedStatementHolderSupplier(transporter, lastSql ) {
                    it.setArray(1, partitions)
                    it.setArray(2, entitySetArray)
                }) {
                    ResultSetAdapters.id(it) to ResultSetAdapters.version(it)
                }.toMap()

                val ekidsArray = PostgresArrays.createUuidArray(conn, idsToVersions.keys)

                entry.value.forEach { (ptId, col) ->
                    logger.info("transporting data rows")
                    lastSql = updateRowsForPropertyType(tableName, ptId, col)
                    val pts = conn.prepareStatement(lastSql)
                    pts.setArray(1, partitions)
                    pts.setArray(2, entitySetArray)
                    pts.setArray(3, ekidsArray)
                    valueCounter.inc(pts.executeUpdate().toDouble())
                }

                logger.info("transporting edge rows")
                lastSql = updateRowsForEdges()
                val edges = conn.prepareStatement( lastSql )
                edges.setArray(1, partitions)
                edges.setArray(2, entitySetArray)
                edges.setArray(3, ekidsArray)
                edges.setArray(4, entitySetArray)
                edges.setArray(5, ekidsArray)
                edges.setArray(6, entitySetArray)
                edges.setArray(7, ekidsArray)
                edgesCounter.inc( edges.executeUpdate().toDouble())

                logger.info("updating last transport timestamps")
                lastSql = updateLastWriteForId()
                val idsCommit = conn.prepareStatement( lastSql )
                idsToVersions.forEach { ( ekid, version ) ->
                    idsCommit.setLong(1, version)
                    idsCommit.setArray(2, partitions)
                    idsCommit.setArray(3, entitySetArray)
                    idsCommit.setObject(4, ekid)
                    idsCommit.addBatch()
                }
                idCounter.inc( idsCommit.executeBatch().sum().toDouble())

                logger.info("Updated {} data rows in entity type table {}", valueCounter.get(), tableName)
                logger.info("Updated {} edge rows for entity type table {}", edgesCounter.get(), tableName)
                logger.info("Updated {} ids for entity type table {}", idCounter.get(), tableName)
            } catch (ex: Exception) {
                errorCounter.inc()
                logger.error("Unable to update transporter: SQL: {}", lastSql, ex)
                throw ex
            }
        }
    }

    override fun getExecutorName(): String {
        return Offloadable.OFFLOADABLE_EXECUTOR
    }

    override fun init(data: TransporterDatastore): TransporterPropagateDataEntryProcessor {
        this.data = data
        return this
    }
}
*/
