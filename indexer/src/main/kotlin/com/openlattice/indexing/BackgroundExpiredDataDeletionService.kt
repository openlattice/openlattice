package com.openlattice.indexing

import com.google.common.base.Stopwatch
import com.google.common.collect.ImmutableMap
import com.google.common.collect.Iterables
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
import com.openlattice.data.*
import com.openlattice.data.DataApi.MAX_BATCH_SIZE
import com.openlattice.datastore.services.EdmManager
import com.openlattice.edm.EntitySet
import com.openlattice.edm.set.EntitySetFlag
import com.openlattice.edm.set.ExpirationBase
import com.openlattice.edm.type.PropertyType
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.indexing.configuration.IndexerConfiguration
import com.openlattice.postgres.DataTables.LAST_WRITE
import com.openlattice.postgres.IndexType
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresDataTables
import com.openlattice.postgres.PostgresTable.DATA
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

class BackgroundExpiredDataDeletionService(
        hazelcastInstance: HazelcastInstance,
        private val indexerConfiguration: IndexerConfiguration,
        private val elasticsearchApi: ConductorElasticsearchApi,
        private val auditingManager: AuditingManager,
        private val dataGraphService: DataGraphService,
        private val edm: EdmManager
) {
    companion object {
        private val logger = LoggerFactory.getLogger(BackgroundExpiredDataDeletionService::class.java)!!
    }

    private val propertyTypes: IMap<UUID, PropertyType> = hazelcastInstance.getMap(HazelcastMap.PROPERTY_TYPES.name)
    private val entitySets: IMap<UUID, EntitySet> = hazelcastInstance.getMap(HazelcastMap.ENTITY_SETS.name)
    private val expirationLocks: IMap<UUID, Long> = hazelcastInstance.getMap(HazelcastMap.EXPIRATION_LOCKS.name)

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

                    val propertiesOfLockedEntitySets = lockedEntitySets
                            .map { it.id to edm.getPropertyTypesAsMap(edm.getEntityType(it.entityTypeId).properties) }
                            .toMap()

                    val totalDeleted = lockedEntitySets
                            .parallelStream()
                            .filter { !it.isLinking }
                            .mapToInt { deleteExpiredData(it, propertiesOfLockedEntitySets[it.id]!!) }
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

    private fun deleteExpiredData(entitySet: EntitySet, propertyTypes: Map<UUID, PropertyType>): Int {
        logger.info(
                "Starting deletion of expired data for entity set {} with id {}",
                entitySet.name,
                entitySet.id
        )

        val deletedEntityKeyIds: MutableSet<UUID> = mutableSetOf()
        var totalDeletedEntitiesCount = 0

        val expirationPT = entitySet.expiration.startDateProperty.map { propertyTypes[it]!! }
        val entityKeyIds = dataGraphService.getExpiringEntitiesFromEntitySet(
                entitySet.id, entitySet.expiration, OffsetDateTime.now(), entitySet.expiration.deleteType, expirationPT
        )
        while (entityKeyIds.iterator().hasNext()) {
            val chunkedEntityKeyIds = Iterables.partition(entityKeyIds, MAX_BATCH_SIZE)
            for (idsChunk in chunkedEntityKeyIds) {
                val ids = idsChunk.toSet()

                val writeEvent = if (entitySet.expiration.deleteType == DeleteType.Hard) {
                    if (!edm.isAssociationEntitySet(entitySet.id)) {
                        deleteAssociationsOfExpiredEntities(entitySet.id, ids)
                    }
                    dataGraphService.deleteEntities(
                            entitySet.id,
                            ids,
                            propertyTypes)

                } else {
                    if (!edm.isAssociationEntitySet(entitySet.id)) {
                        clearAssociationsOfExpiredEntities(entitySet.id, ids)
                    }
                    dataGraphService.clearEntities(
                            entitySet.id,
                            ids,
                            propertyTypes)
                }

                logger.info("Completed deleting {} expired elements from entity set {}.",
                        writeEvent.numUpdates,
                        entitySet.name)

                totalDeletedEntitiesCount += writeEvent.numUpdates
                deletedEntityKeyIds.addAll(ids)
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

    private fun deleteAssociationsOfExpiredEntities(entitySetId: UUID, ids: Set<UUID>): List<WriteEvent> {
        val associationEdgeKeys: PostgresIterable<DataEdgeKey> = dataGraphService.getEdgesConnectedToEntities(entitySetId, ids, true)
        val associationPropertyTypes = associationEdgeKeys
                .map {
                    it.edge.entitySetId to
                            edm.getPropertyTypesAsMap(
                                    edm.getEntityType(edm.getEntitySet(it.edge.entitySetId).entityTypeId
                                    ).properties
                            )
                }.toMap()
        return dataGraphService.deleteAssociationsBatch(entitySetId, associationEdgeKeys, associationPropertyTypes)
    }

    private fun clearAssociationsOfExpiredEntities(entitySetId: UUID, ids: Set<UUID>): List<WriteEvent> {
        val associationEdgeKeys: PostgresIterable<DataEdgeKey> = dataGraphService.getEdgesConnectedToEntities(entitySetId, ids, false)

        //filter out audit entity set edges from association edges to clear
        val associationEdgeESIds = associationEdgeKeys.map { it.edge.entitySetId }.toSet()
        val auditEdgeEntitySetIds = edm.getEntitySetIdsWithFlags(associationEdgeESIds, setOf(EntitySetFlag.AUDIT))
        val filteredAssociationEdgeKeys = associationEdgeKeys
                .filter { auditEdgeEntitySetIds.contains(it.edge.entitySetId) }
                .toSet()

        val associationPropertyTypes = filteredAssociationEdgeKeys
                .map {
                    it.edge.entitySetId to
                            edm.getPropertyTypesAsMap(
                                    edm.getEntityType(edm.getEntitySet(it.edge.entitySetId).entityTypeId
                                    ).properties
                            )
                }.toMap()
        return dataGraphService.clearAssociationsBatch(entitySetId, associationEdgeKeys, associationPropertyTypes)
    }

    private fun tryLockEntitySet(entitySet: EntitySet): Boolean {
        return expirationLocks.putIfAbsent(entitySet.id, System.currentTimeMillis() + MAX_DURATION_MILLIS) == null
    }

    private fun deleteIndexingLock(entitySet: EntitySet) {
        expirationLocks.delete(entitySet.id)
    }

}