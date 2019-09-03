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
import com.zaxxer.hikari.HikariDataSource
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import java.time.Instant
import java.time.LocalDate
import java.time.OffsetDateTime
import java.time.ZoneId
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock

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
                            "Completed deleting {} expired elements in {} ms",
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

    /*
    private fun getEntityDataKeysQuery(entitySetId: UUID): String {
        return "SELECT ${ID.name}, ${LAST_WRITE.name} FROM ${IDS.name} " +
                "WHERE ${ENTITY_SET_ID.name} = '$entitySetId' AND ${VERSION.name} > 0"
    }

    private fun getDirtyEntitiesWithLastWriteQuery(entitySetId: UUID): String {
        return "SELECT ${ID.name}, ${LAST_WRITE.name} FROM ${IDS.name} " +
                "WHERE ${ENTITY_SET_ID.name} = '$entitySetId' AND " +
                "${LAST_INDEX.name} < ${LAST_WRITE.name} AND " +
                "${VERSION.name} > 0 " +
                "LIMIT $FETCH_SIZE"
    }

    private fun getEntityDataKeys(entitySetId: UUID): PostgresIterable<Pair<UUID, OffsetDateTime>> {
        return PostgresIterable(Supplier<StatementHolder> {
            val connection = hds.connection
            connection.autoCommit = false
            val stmt = connection.createStatement()
            stmt.fetchSize = 64_000
            val rs = stmt.executeQuery(getEntityDataKeysQuery(entitySetId))
            StatementHolder(connection, stmt, rs)
        }, Function<ResultSet, Pair<UUID, OffsetDateTime>> {
            ResultSetAdapters.id(it) to ResultSetAdapters.lastWriteTyped(it)
        })
    }

    private fun getDirtyEntityKeyIds(entitySetId: UUID): PostgresIterable<Pair<UUID, OffsetDateTime>> {
        return PostgresIterable(Supplier<StatementHolder> {
            val connection = hds.connection
            val stmt = connection.createStatement()
            val rs = stmt.executeQuery(getDirtyEntitiesWithLastWriteQuery(entitySetId))
            StatementHolder(connection, stmt, rs)
        }, Function<ResultSet, Pair<UUID, OffsetDateTime>> {
            ResultSetAdapters.id(it) to ResultSetAdapters.lastWriteTyped(it)
        })
    }

    private fun getPropertyTypeForEntityType(entityTypeId: UUID): Map<UUID, PropertyType> {
        return propertyTypes
                .getAll(entityTypes[entityTypeId]?.properties ?: setOf())
                .filter { it.value.datatype != EdmPrimitiveTypeKind.Binary }
    }
    */

    //deletes expired data from data and ids table in postgres and elasticsearch
    private fun deleteExpiredData(entitySet: EntitySet): Int {
        logger.info(
                "Starting indexing expired data for entity set {} with id {}",
                entitySet.name,
                entitySet.id
        )

        val esw = Stopwatch.createStarted()
        val pair: Pair<Set<UUID>, Int> = deleteExpiredDataFromDataTable(entitySet.id, entitySet.expiration)

        val dataTableDeleteCount = pair.second
        val expiredEntityKeyIds = pair.first
        var elasticsearchDataDeleted = false
        var idsTableDeleteCount = 0
        if (expiredEntityKeyIds.isNotEmpty()) {
            elasticsearchDataDeleted = delteExpiredDataFromElasticSearch(entitySet.id, entitySet.entityTypeId, expiredEntityKeyIds)
            idsTableDeleteCount = deleteExpiredDataFromIdTable(expiredEntityKeyIds)
        }

        /*
        val entityKeyIdsWithLastWrite = if (reindexAll) {
            getEntityDataKeys(entitySet.id)
        } else {
            getDirtyEntityKeyIds(entitySet.id)
        }

        val propertyTypes = getPropertyTypeForEntityType(entitySet.entityTypeId)

        var indexCount = 0
        var entityKeyIdsIterator = entityKeyIdsWithLastWrite.iterator()

        while (entityKeyIdsIterator.hasNext()) {
            updateExpiration(entitySet)
            while (entityKeyIdsIterator.hasNext()) {
                val batch = getBatch(entityKeyIdsIterator)
                indexCount += indexEntities(entitySet, batch, propertyTypes, !reindexAll)
            }
            entityKeyIdsIterator = entityKeyIdsWithLastWrite.iterator()
        }

        logger.info(
                "Finished indexing {} elements from entity set {} in {} ms",
                deletionCount,
                entitySet.name,
                esw.elapsed(TimeUnit.MILLISECONDS)
        )
        */

        //compare deletion from data table and ids table and whatever is going on in elasticsearch
        check(dataTableDeleteCount == idsTableDeleteCount) { "Number of entities deleted from data and ids table are not the same. UH OH." } //do something better
        check(elasticsearchDataDeleted) { "Expired data not deleted from elasticsearch. UH OH." } // also do something better

        return dataTableDeleteCount
    }

    private fun deleteExpiredDataFromDataTable(entitySetId: UUID, expiration: DataExpiration): Pair<Set<UUID>, Int> {
        val connection = hds.connection
        connection.autoCommit = false
        val dataTableDeleteStmt = connection.prepareStatement(getDeleteQuery(entitySetId))
        val expiredIdsStmt = connection.prepareStatement(getExpiredIdsQuery(entitySetId))
        when (expiration.expirationFlag) {
            ExpirationType.DATE_PROPERTY -> {
                val propertyTypeId: UUID = expiration.startDateProperty.get()
                val propertyType = propertyTypes[propertyTypeId]
                dataTableDeleteStmt.setObject(1, propertyTypeId)
                expiredIdsStmt.setObject(1, propertyTypeId)
                when {
                    propertyType!!.datatype == EdmPrimitiveTypeKind.Date -> {
                        val elapsedTime = OffsetDateTime.ofInstant(Instant.now().minusMillis(expiration.timeToExpiration), ZoneId.systemDefault()).toLocalDate()
                        dataTableDeleteStmt.setObject(2, elapsedTime)
                        expiredIdsStmt.setObject(2, elapsedTime)
                    }
                    propertyType!!.datatype == EdmPrimitiveTypeKind.DateTimeOffset -> {
                        val elapsedTime = OffsetDateTime.ofInstant(Instant.now().minusMillis(expiration.timeToExpiration), ZoneId.systemDefault())
                        dataTableDeleteStmt.setObject(2, elapsedTime)
                        expiredIdsStmt.setObject(2, elapsedTime)
                    }
                    else -> {
                        logger.error("Something terrible has happened.")
                    }
                }
            }
            ExpirationType.FIRST_WRITE -> {
                val elapsedTime = Instant.now().minusMillis(expiration.timeToExpiration).toEpochMilli()
                dataTableDeleteStmt.setObject(1, "${VERSIONS.name}[1]")
                dataTableDeleteStmt.setObject(2, elapsedTime)
                expiredIdsStmt.setObject(1, "${VERSIONS.name}[1]")
                expiredIdsStmt.setObject(2, elapsedTime)
            }
            ExpirationType.LAST_WRITE -> {
                val elapsedTime = OffsetDateTime.ofInstant(Instant.now().minusMillis(expiration.timeToExpiration), ZoneId.systemDefault())
                dataTableDeleteStmt.setObject(1, "${LAST_WRITE.name}")
                dataTableDeleteStmt.setObject(2, elapsedTime)
                expiredIdsStmt.setObject(1, "${LAST_WRITE.name}")
                expiredIdsStmt.setObject(2, elapsedTime)
            }
            else -> logger.info( "No data has expired.")
        }
        val expiredEntityKeyIds = ResultSetAdapters.entityKeyIds(expiredIdsStmt.executeQuery())
        val deleteCount = dataTableDeleteStmt.executeUpdate()
        connection.commit()
        return Pair(expiredEntityKeyIds, deleteCount)
    }

    private fun delteExpiredDataFromElasticSearch(entitySetId: UUID, entityTypeId: UUID, expiredEntityKeyIds: Set<UUID>): Boolean {
        return elasticsearchApi.deleteEntityDataBulk(entitySetId, entityTypeId, expiredEntityKeyIds)
    }

    private fun deleteExpiredDataFromIdTable(expiredEntityKeyIds: Set<UUID>): Int {
        hds.connection.use { conn ->
            val stmt = conn.createStatement()
            val expiredIdsAsString = expiredEntityKeyIds.joinToString(prefix = "('", postfix = "')", separator = "', '") { it.toString() }
            return stmt.executeUpdate(deleteExpiredDataFromIdTableQuery(expiredIdsAsString))
        }
    }

    private fun getDeleteQuery(entitySetId: UUID): String {
        return "DELETE FROM ${DATA.name} WHERE ${ENTITY_SET_ID.name} = '$entitySetId' AND ? < ?"
    }

    private fun getExpiredIdsQuery(entitySetId: UUID): String {
        return "SELECT ${ID.name} FROM ${DATA.name} WHERE ${ENTITY_SET_ID.name} = '$entitySetId' AND ? < ?"
    }

    private fun deleteExpiredDataFromIdTableQuery(expiredIds: String): String {
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