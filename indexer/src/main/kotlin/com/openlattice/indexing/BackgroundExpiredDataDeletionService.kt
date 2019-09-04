package com.openlattice.indexing

import com.google.common.base.Stopwatch
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.core.IMap
import com.hazelcast.query.Predicate
import com.hazelcast.query.Predicates
import com.hazelcast.query.QueryConstants
import com.openlattice.conductor.rpc.ConductorElasticsearchApi
import com.openlattice.data.DataExpiration
import com.openlattice.data.storage.IndexingMetadataManager
import com.openlattice.data.storage.PostgresEntityDataQueryService
import com.openlattice.edm.EntitySet
import com.openlattice.edm.set.ExpirationType
import com.openlattice.edm.type.EntityType
import com.openlattice.edm.type.PropertyType
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.indexing.configuration.IndexerConfiguration
import com.openlattice.postgres.DataTables.LAST_WRITE
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresTable.DATA
import com.openlattice.postgres.PostgresTable.IDS
import com.openlattice.postgres.ResultSetAdapters
import com.openlattice.postgres.streams.PostgresIterable
import com.openlattice.postgres.streams.StatementHolder
import com.zaxxer.hikari.HikariDataSource
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import java.sql.Types
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.time.Instant
import java.time.LocalDate
import java.time.OffsetDateTime
import java.time.ZoneId
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import java.util.function.Function
import java.util.function.Supplier

/**
 *
 */

const val MAX_DURATION_MILLIS = 60000L
const val DATA_DELETION_RATE = 30000L

class BackgroundExpiredDataDeletionService(
        hazelcastInstance: HazelcastInstance,
        private val indexerConfiguration: IndexerConfiguration,
        private val hds: HikariDataSource,
        private val elasticsearchApi: ConductorElasticsearchApi
) {
    companion object {
        private val logger = LoggerFactory.getLogger(BackgroundExpiredDataDeletionService::class.java)!!
    }

    private val propertyTypes: IMap<UUID, PropertyType> = hazelcastInstance.getMap(HazelcastMap.PROPERTY_TYPES.name)
    private val entitySets: IMap<UUID, EntitySet> = hazelcastInstance.getMap(HazelcastMap.ENTITY_SETS.name)
    private val expirationLocks: IMap<UUID, Long> = hazelcastInstance.getMap(HazelcastMap.EXPIRATION_LOCKS.name)

    //necessary??????
    init {
        expirationLocks.addIndex(QueryConstants.THIS_ATTRIBUTE_NAME.value(), true)
    }

    private val taskLock = ReentrantLock()

    @Suppress("UNCHECKED_CAST", "UNUSED")
    @Scheduled(fixedRate = MAX_DURATION_MILLIS)
    fun scavengeIndexingLocks() {
        expirationLocks.removeAll(
                Predicates.lessThan(
                        QueryConstants.THIS_ATTRIBUTE_NAME.value(),
                        System.currentTimeMillis()
                ) as Predicate<UUID, Long>
        )
    }

    @Suppress("UNUSED")
    @Scheduled(fixedRate = DATA_DELETION_RATE)
    fun deleteExpiredDataFromEntitySets() {
        logger.info("Starting background expired data deletion task.")
        //Keep number of expired data deletion jobs under control
        if (taskLock.tryLock()) {
            try {
                if (indexerConfiguration.backgroundExpiredDataDeletionEnabled) {
                    val w = Stopwatch.createStarted()
                    //We shuffle entity sets to make sure we have a chance to work share and index everything
                    //lock entity sets we are working on
                    val lockedEntitySets = entitySets.values
                            .shuffled()
                            .filter { tryLockEntitySet(it) } //filters out entitysets that are already locked, and the entitysets we're working on are now locked in the IMap
                            .filter { it.name != "OpenLattice Audit Entity Set" } //TODO: Clean out audit entity set from prod

                    //delete expired data
                    val totalDeleted = lockedEntitySets
                            .parallelStream()
                            .filter { !it.isLinking }
                            .mapToInt { deleteExpiredData(it) }
                            .sum()

                    //unlock the entitysets we were working on
                    lockedEntitySets.forEach(this::deleteIndexingLock)

                    logger.info(
                            "Completed deleting {} expired elements in {} ms.",
                            totalDeleted,
                            w.elapsed(TimeUnit.MILLISECONDS)
                    )
                } else {
                    logger.info("Skipping expired data deletion as it is not enabled.")
                }
            } finally {
                taskLock.unlock()
            }
        } else {
            logger.info("Not starting new expired data deletion job as an existing one is running.")
        }
    }

    //deletes expired data from data and ids table in postgres and elasticsearch
    private fun deleteExpiredData(entitySet: EntitySet): Int {
        logger.info(
                "Starting deletion of expired data for entity set {} with id {}",
                entitySet.name,
                entitySet.id
        )
        var dataTableDeleteCount = 0
        if (entitySet.expiration != null) {
            val esw = Stopwatch.createStarted()
            val pair: Pair<Set<UUID>, Int> = deleteExpiredDataFromDataTable(entitySet.id, entitySet.expiration)

            dataTableDeleteCount = pair.second
            if (dataTableDeleteCount > 0) {
                val expiredEntityKeyIds = pair.first
                var elasticsearchDataDeleted = false
                var idsTableDeleteCount = 0
                if (expiredEntityKeyIds.isNotEmpty()) {
                    elasticsearchDataDeleted = deleteExpiredDataFromElasticSearch(entitySet.id, entitySet.entityTypeId, expiredEntityKeyIds)
                    idsTableDeleteCount = deleteExpiredDataFromIdsTable(expiredEntityKeyIds)
                }

                //compare deletion from data table and ids table and whether data was deleted from elasticsearch
                check(dataTableDeleteCount == idsTableDeleteCount) { "Number of entities deleted from data and ids table are not the same. UH OH." } //do something better
                check(elasticsearchDataDeleted) { "Expired data not deleted from elasticsearch. UH OH." } // also do something better
                logger.info("Completed deleting {} expired elements from entity set {}.",
                        dataTableDeleteCount,
                        entitySet.name)
            }

        } else {
            logger.info(
                    "Entity set {} does not have a data expiration policy.",
                    entitySet.name
            )
        }
        return dataTableDeleteCount
    }

    private fun deleteExpiredDataFromDataTable(entitySetId: UUID, expiration: DataExpiration): Pair<Set<UUID>, Int> {
        val connection = hds.connection
        connection.autoCommit = false
        val dataTableDeleteStmt = connection.prepareStatement(deleteExpiredDataFromDataTableQuery(entitySetId))
        val comparisonField: String
        val expirationField: Any
        val expirationFieldSQLType: Int
        when (expiration.expirationFlag) {
            ExpirationType.DATE_PROPERTY -> {
                val propertyTypeId: UUID = expiration.startDateProperty.get()
                val propertyType = propertyTypes[propertyTypeId]
                comparisonField = "'$propertyTypeId'"
                dataTableDeleteStmt.setObject(1, comparisonField)
                if (propertyType!!.datatype == EdmPrimitiveTypeKind.Date) {
                    expirationField = OffsetDateTime.ofInstant(Instant.now().minusMillis(expiration.timeToExpiration), ZoneId.systemDefault()).toLocalDate()
                    expirationFieldSQLType = Types.DATE
                    dataTableDeleteStmt.setObject(2, expirationField, expirationFieldSQLType)
                } else {  //only other TypeKind for date property type is OffsetDateTime
                    expirationField = OffsetDateTime.ofInstant(Instant.now().minusMillis(expiration.timeToExpiration), ZoneId.systemDefault())
                    expirationFieldSQLType = Types.TIMESTAMP_WITH_TIMEZONE
                    dataTableDeleteStmt.setObject(2, expirationField, expirationFieldSQLType)
                }
            }
            ExpirationType.FIRST_WRITE -> {
                expirationField = Instant.now().minusMillis(expiration.timeToExpiration).toEpochMilli()
                expirationFieldSQLType = Types.BIGINT
                dataTableDeleteStmt.setObject(1, "${VERSIONS.name}[1]")
                dataTableDeleteStmt.setObject(2, expirationField, expirationFieldSQLType)
                comparisonField = "${VERSIONS.name}[1]"
            }
            ExpirationType.LAST_WRITE -> {
                expirationField = OffsetDateTime.ofInstant(Instant.now().minusMillis(expiration.timeToExpiration), ZoneId.systemDefault())
                expirationFieldSQLType = Types.TIMESTAMP_WITH_TIMEZONE
                dataTableDeleteStmt.setObject(1, "${LAST_WRITE.name}")
                dataTableDeleteStmt.setObject(2, expirationField, expirationFieldSQLType)
                comparisonField = "${LAST_WRITE.name}"
            }
        }
        val expiredEntityKeyIds = getExpiredIds(comparisonField, expirationField, expirationFieldSQLType, entitySetId)
        val deleteCount = dataTableDeleteStmt.executeUpdate()
        connection.commit()
        return Pair(expiredEntityKeyIds.toSet(), deleteCount)
    }

    private fun getExpiredIds(comparisonField: String, expirationField: Any, expirationFieldSQLType: Int, entitySetId: UUID): PostgresIterable<UUID> {
        return PostgresIterable(
                Supplier {
                    val connection = hds.connection
                    val stmt = connection.prepareStatement(getExpiredIdsQuery(entitySetId))
                    stmt.setString(1, comparisonField)
                    stmt.setObject(2, expirationField, expirationFieldSQLType)
                    StatementHolder(connection, stmt, stmt.executeQuery())
                },
                Function<ResultSet, UUID> { ResultSetAdapters.id(it) }
        )
    }

    private fun deleteExpiredDataFromElasticSearch(entitySetId: UUID, entityTypeId: UUID, expiredEntityKeyIds: Set<UUID>): Boolean {
        return elasticsearchApi.deleteEntityDataBulk(entitySetId, entityTypeId, expiredEntityKeyIds)
    }

    private fun deleteExpiredDataFromIdsTable(expiredEntityKeyIds: Set<UUID>): Int {
        hds.connection.use { conn ->
            val stmt = conn.createStatement()
            val expiredIdsAsString = expiredEntityKeyIds.joinToString(prefix = "('", postfix = "')", separator = "', '") { it.toString() }
            return stmt.executeUpdate(deleteExpiredDataFromIdsTableQuery(expiredIdsAsString))
        }
    }

    private fun deleteExpiredDataFromDataTableQuery(entitySetId: UUID): String {
        return "DELETE FROM ${DATA.name} WHERE ${ENTITY_SET_ID.name} = '$entitySetId' AND ? < ?"
    }

    private fun getExpiredIdsQuery(entitySetId: UUID): String {
        return "SELECT ${ID.name} FROM ${DATA.name} WHERE ${ENTITY_SET_ID.name} = '$entitySetId' AND ? < ?"
    }

    private fun deleteExpiredDataFromIdsTableQuery(expiredIds: String): String {
        return "DELETE FROM ${IDS.name} WHERE ${ID.name} IN $expiredIds"
    }


    private fun tryLockEntitySet(entitySet: EntitySet): Boolean {
        return expirationLocks.putIfAbsent(entitySet.id, System.currentTimeMillis() + MAX_DURATION_MILLIS) == null
        //putifabsent returns null if there was no value in the map. ie entityset was not locked
        //method will return true if the entity set was not locked, and means the entityset is now locked
        //method will return false if the entityset was already locked
    }

    private fun deleteIndexingLock(entitySet: EntitySet) {
        expirationLocks.delete(entitySet.id)
    }

    /*
    private fun updateExpiration(entitySet: EntitySet) {
        expirationLocks.set(entitySet.id, System.currentTimeMillis() + MAX_DURATION_MILLIS)
    }
    */

    /*
    private fun getBatch(entityKeyIdStream: Iterator<Pair<UUID, OffsetDateTime>>): Map<UUID, OffsetDateTime> {
        val entityKeyIds = HashMap<UUID, OffsetDateTime>(INDEX_SIZE)

        var i = 0
        while (entityKeyIdStream.hasNext() && i < INDEX_SIZE) {
            val entityWithLastWrite = entityKeyIdStream.next()
            entityKeyIds[entityWithLastWrite.first] = entityWithLastWrite.second
            ++i
        }

        return entityKeyIds
    }
    */
}