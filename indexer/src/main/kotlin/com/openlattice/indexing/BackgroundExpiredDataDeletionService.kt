package com.openlattice.indexing

import com.google.common.base.Stopwatch
import com.google.common.collect.ImmutableMap
import com.hazelcast.config.IndexType
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.query.QueryConstants
import com.openlattice.IdConstants
import com.openlattice.auditing.AuditEventType
import com.openlattice.auditing.AuditableEvent
import com.openlattice.auditing.AuditingManager
import com.openlattice.authorization.AclKey
import com.openlattice.data.DataDeletionManager
import com.openlattice.data.DataGraphManager
import com.openlattice.datastore.services.EdmManager
import com.openlattice.datastore.services.EntitySetManager
import com.openlattice.edm.EntitySet
import com.openlattice.edm.type.PropertyType
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.indexing.configuration.IndexerConfiguration
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import java.time.OffsetDateTime
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import javax.inject.Inject

/**
 * This is a background task that periodically searches for data that has surpassed its prescribed expiration date
 * and removes expired data from postgres and elasticsearch
 */

const val MAX_DURATION_MILLIS = 60_000L
const val DATA_DELETION_RATE = 30_000L

class BackgroundExpiredDataDeletionService(
        hazelcastInstance: HazelcastInstance,
        private val indexerConfiguration: IndexerConfiguration,
        private val auditingManager: AuditingManager,
        private val dataGraphService: DataGraphManager,
        private val deletionManager: DataDeletionManager,
        private val entitySetManager: EntitySetManager
) {
    companion object {
        private val logger = LoggerFactory.getLogger(BackgroundExpiredDataDeletionService::class.java)!!
    }

    private val entitySets = HazelcastMap.ENTITY_SETS.getMap(hazelcastInstance)
    private val expirationLocks = HazelcastMap.BACKGROUND_EXPIRED_DATA_DELETION_LOCKS.getMap(hazelcastInstance)

    @Inject
    private lateinit var edm: EdmManager

    init {
        expirationLocks.addIndex(IndexType.SORTED, QueryConstants.THIS_ATTRIBUTE_NAME.value())
    }

    private val taskLock = ReentrantLock()

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

                    val totalDeleted = lockedEntitySets
                            .parallelStream()
                            .filter { !it.isLinking }
                            .mapToInt {
                                try {
                                    deleteExpiredData(it)
                                } catch (e: Exception) {
                                    logger.error("An error occurred while trying to delete expired data for entity set {}", it.id, e)
                                    0
                                }
                            }
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

    private fun getBatchOfExpiringEkids(entitySet: EntitySet, expirationPT: Optional<PropertyType>): MutableSet<UUID> {
        return dataGraphService.getExpiringEntitiesFromEntitySet(
                entitySet.id,
                entitySet.expiration!!,
                OffsetDateTime.now(),
                entitySet.expiration!!.deleteType,
                expirationPT
        ).toMutableSet()
    }

    private fun deleteExpiredData(entitySet: EntitySet): Int {
        logger.info(
                "Starting deletion of expired data for entity set {} with id {}",
                entitySet.name,
                entitySet.id
        )

        val propertyTypes = entitySetManager.getPropertyTypesForEntitySet(entitySet.id)

        var totalDeletedEntitiesCount = 0

        val expirationPT = entitySet.expiration!!.startDateProperty.map { propertyTypes[it]!! }
        var idsBatch = getBatchOfExpiringEkids(entitySet, expirationPT)

        while (idsBatch.isNotEmpty()) {

            val writeEvent = deletionManager.clearOrDeleteEntities(
                    entitySet.id,
                    idsBatch,
                    entitySet.expiration!!.deleteType
            )

            logger.info(
                    "Completed deleting {} expired elements from entity set {}.",
                    writeEvent.numUpdates,
                    entitySet.name
            )

            totalDeletedEntitiesCount += writeEvent.numUpdates

            auditingManager.recordEvents(
                    listOf(
                            AuditableEvent(
                                    IdConstants.SYSTEM_ID.id,
                                    AclKey(entitySet.id),
                                    AuditEventType.DELETE_EXPIRED_ENTITIES,
                                    "Expired entities deleted through BackgroundExpiredDataDeletionService",
                                    Optional.of(idsBatch),
                                    ImmutableMap.of(),
                                    OffsetDateTime.now(),
                                    Optional.empty()
                            )
                    )
            )

            idsBatch = getBatchOfExpiringEkids(entitySet, expirationPT)
        }

        return totalDeletedEntitiesCount
    }

    private fun tryLockEntitySet(entitySet: EntitySet): Boolean {
        return expirationLocks.putIfAbsent(entitySet.id, System.currentTimeMillis() + MAX_DURATION_MILLIS) == null
    }

    private fun deleteIndexingLock(entitySet: EntitySet) {
        expirationLocks.delete(entitySet.id)
    }

}