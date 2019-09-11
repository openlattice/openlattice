package com.openlattice.indexing

import com.google.common.base.Stopwatch
import com.google.common.collect.ImmutableMap
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.core.IMap
import com.hazelcast.query.Predicate
import com.hazelcast.query.Predicates
import com.hazelcast.query.QueryConstants
import com.openlattice.IdConstants
import com.openlattice.auditing.AuditEventType
import com.openlattice.auditing.AuditableEvent
import com.openlattice.auditing.AuditingManager
import com.openlattice.authorization.AclKey
import com.openlattice.conductor.rpc.ConductorElasticsearchApi
import com.openlattice.data.DataExpiration
import com.openlattice.edm.EntitySet
import com.openlattice.edm.set.ExpirationBase
import com.openlattice.edm.type.PropertyType
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.indexing.configuration.IndexerConfiguration
import com.openlattice.postgres.DataTables.LAST_WRITE
import com.openlattice.postgres.IndexType
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresDataTables
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
import java.sql.ResultSet
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneId
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import java.util.function.Function
import java.util.function.Supplier

/**
 * This is a background task that periodically searches for data that has surpassed its prescribed expiration date
 * and removes expired data from postgres and elasticsearch
 */

const val MAX_DURATION_MILLIS = 60_000L
const val DATA_DELETION_RATE = 30_000L

class BackgroundExpiredDataDeletionService (
        hazelcastInstance: HazelcastInstance,
        private val indexerConfiguration: IndexerConfiguration,
        private val hds: HikariDataSource,
        private val elasticsearchApi: ConductorElasticsearchApi,
        private val auditingManager: AuditingManager
) {
    companion object {
        private val logger = LoggerFactory.getLogger(BackgroundExpiredDataDeletionService::class.java)!!
    }

    private val propertyTypes: IMap<UUID, PropertyType> = hazelcastInstance.getMap(HazelcastMap.PROPERTY_TYPES.name)
    private val entitySets: IMap<UUID, EntitySet> = hazelcastInstance.getMap(HazelcastMap.ENTITY_SETS.name)
    private val expirationLocks: IMap<UUID, Long> = hazelcastInstance.getMap(HazelcastMap.EXPIRATION_LOCKS.name)
    private var entitySetToPartition: Map<UUID, Set<Int>> = mapOf()
    private var propertyTypeIdToColumnData: Map<UUID, Pair<IndexType,EdmPrimitiveTypeKind>> = mapOf()

    init {
        expirationLocks.addIndex(QueryConstants.THIS_ATTRIBUTE_NAME.value(), true)
    }

    private val taskLock = ReentrantLock()

    @Suppress("UNCHECKED_CAST", "UNUSED")
    @Scheduled(fixedRate = MAX_DURATION_MILLIS)
    fun scavengeExpirationLocks() {
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
                    val lockedEntitySets = entitySets.values
                            .filter { it.hasExpirationPolicy() }
                            .filter { tryLockEntitySet(it) }
                            .shuffled()

                    entitySetToPartition = lockedEntitySets.map { it.id to it.partitions }.toMap()
                    val propertyTypeIds = lockedEntitySets.filter { it.expiration.startDateProperty.isPresent }.map { it.expiration.startDateProperty.get() }.toSet()
                    if (propertyTypeIds.isNotEmpty()) {
                        propertyTypeIdToColumnData = propertyTypes.map{it.key to Pair(it.value.postgresIndexType, it.value.datatype)}.toMap()
                    }
                    
                    val totalDeleted = lockedEntitySets
                            .parallelStream()
                            .filter { !it.isLinking }
                            .mapToInt { deleteExpiredData(it) }
                            .sum()

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

    private fun deleteExpiredData(entitySet: EntitySet): Int {
        logger.info(
                "Starting deletion of expired data for entity set {} with id {}",
                entitySet.name,
                entitySet.id
        )

        val deletedEntityKeyIds: MutableSet<UUID> = mutableSetOf()
        var totalDeletedEntitiesCount = 0
        val sqlParams = getSqlParameters(entitySet.expiration)
        val partitions = (entitySetToPartition[entitySet.id]
                ?: error("No partitions assigned")).joinToString(prefix = "('", postfix = "')", separator = "', '") { it.toString() }

        var entityKeyIds = getExpiredIds(entitySet.id, sqlParams, partitions).toSet()
        if ( entityKeyIds.isNotEmpty() ) {  //if no data have expired let's not waste our time
            while ( entityKeyIds.isNotEmpty() ) {
                val thisDeletedEntitiesCount = deleteExpiredEntities(entitySet, entityKeyIds, partitions)
                logger.info("Completed deleting {} expired elements from entity set {}.",
                        thisDeletedEntitiesCount,
                        entitySet.name)
                totalDeletedEntitiesCount += thisDeletedEntitiesCount
                deletedEntityKeyIds.addAll(entityKeyIds)
                entityKeyIds = getExpiredIds(entitySet.id, sqlParams, partitions).toSet()
            }

            auditingManager.recordEvents(
                    listOf(AuditableEvent(
                            IdConstants.SYSTEM_ID.id,
                            AclKey(entitySet.id),
                            AuditEventType.DELETE_EXPIRED_ENTITIES,
                            "Expired entities deleted through BackgroundExpiredDataDeletionService",
                            Optional.of(deletedEntityKeyIds),
                            ImmutableMap.of(),
                            OffsetDateTime.now(),
                            Optional.empty())
                    )
            )
        }
        return totalDeletedEntitiesCount
    }

    private fun getSqlParameters(expiration: DataExpiration) : Triple<String, Any, Int> {
        val comparisonField: String
        val expirationField: Any
        val expirationFieldSQLType: Int
        val expirationInstant = Instant.now().minusMillis(expiration.timeToExpiration)
        when (expiration.expirationBase) {
            ExpirationBase.DATE_PROPERTY -> {
                val columnData = propertyTypeIdToColumnData.getValue(expiration.startDateProperty.get())
                comparisonField = PostgresDataTables.getColumnDefinition(columnData.first, columnData.second).name
                if (columnData.second == EdmPrimitiveTypeKind.Date) {
                    expirationField = OffsetDateTime.ofInstant(expirationInstant, ZoneId.systemDefault()).toLocalDate()
                    expirationFieldSQLType = Types.DATE
                } else {  //only other TypeKind for date property type is OffsetDateTime
                    expirationField = OffsetDateTime.ofInstant(expirationInstant, ZoneId.systemDefault())
                    expirationFieldSQLType = Types.TIMESTAMP_WITH_TIMEZONE
                }
            }
            ExpirationBase.FIRST_WRITE -> {
                expirationField = expirationInstant.toEpochMilli()
                expirationFieldSQLType = Types.BIGINT
                comparisonField = "${VERSIONS.name}[1]"
            }
            ExpirationBase.LAST_WRITE -> {
                expirationField = OffsetDateTime.ofInstant(expirationInstant, ZoneId.systemDefault())
                expirationFieldSQLType = Types.TIMESTAMP_WITH_TIMEZONE
                comparisonField = LAST_WRITE.name
            }
        }
        return Triple(comparisonField, expirationField, expirationFieldSQLType)
    }

    private fun getExpiredIds(entitySetId: UUID, sqlParams: Triple<String, Any, Int>, partitions: String): PostgresIterable<UUID> {
        return PostgresIterable(
                Supplier {
                    val connection = hds.connection
                    val stmt = connection.prepareStatement(getExpiredIdsQuery(entitySetId, sqlParams.first, partitions))
                    stmt.setObject(1, sqlParams.second, sqlParams.third)
                    StatementHolder(connection, stmt, stmt.executeQuery())
                },
                Function<ResultSet, UUID> { ResultSetAdapters.id(it) }
        )
    }

    private fun deleteExpiredEntities(entitySet: EntitySet, entityKeyIds: Set<UUID>, partitions: String) : Int {
        var elasticsearchDataDeleted = false
        val expiredIdsAsString = entityKeyIds.joinToString(prefix = "('", postfix = "')", separator = "', '") { it.toString() }
        val connection = hds.connection
        connection.autoCommit = false
        val dataTableDeleteStmt = connection.prepareStatement(deleteExpiredDataFromDataTableQuery(entitySet.id, expiredIdsAsString, partitions))
        val deleteCount = dataTableDeleteStmt.executeUpdate()
        val idsTableDeleteStmt = connection.prepareStatement(deleteExpiredDataFromIdsTableQuery(expiredIdsAsString))
        val idsTableDeleteCount = idsTableDeleteStmt.executeUpdate()
        connection.commit()

        elasticsearchDataDeleted = deleteExpiredDataFromElasticSearch(entitySet.id, entitySet.entityTypeId, entityKeyIds)
        check(elasticsearchDataDeleted) { "Expired data not deleted from elasticsearch. UH OH." } // also do something better

        return idsTableDeleteCount
    }

    private fun deleteExpiredDataFromElasticSearch(entitySetId: UUID, entityTypeId: UUID, expiredEntityKeyIds: Set<UUID>): Boolean {
        return elasticsearchApi.deleteEntityDataBulk(entitySetId, entityTypeId, expiredEntityKeyIds)
    }

    private fun getExpiredIdsQuery(entitySetId: UUID, comparisonField: String, partitions: String): String {
        return "SELECT ${ID.name} FROM ${DATA.name} " +
                "WHERE ${ENTITY_SET_ID.name} = '$entitySetId' " +
                "AND ${PARTITION.name} IN $partitions " +
                "AND $comparisonField < ? " +
                "AND versions[1] >= 0 " +
                "LIMIT $FETCH_SIZE"
    }

    private fun deleteExpiredDataFromDataTableQuery(entitySetId: UUID, expiredIds: String, partitions: String): String {
        return "DELETE FROM ${DATA.name} " +
                "WHERE ${ENTITY_SET_ID.name} = '$entitySetId' " +
                "AND ${PARTITION.name} IN $partitions " +
                "AND ${ID.name} IN $expiredIds"
    }

    private fun deleteExpiredDataFromIdsTableQuery(expiredIds: String): String {
        return "DELETE FROM ${IDS.name} WHERE ${ID.name} IN $expiredIds"
    }

    private fun tryLockEntitySet(entitySet: EntitySet): Boolean {
        return expirationLocks.putIfAbsent(entitySet.id, System.currentTimeMillis() + MAX_DURATION_MILLIS) == null
    }

    private fun deleteIndexingLock(entitySet: EntitySet) {
        expirationLocks.delete(entitySet.id)
    }

}