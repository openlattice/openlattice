package com.openlattice.indexing

import com.geekbeast.rhizome.jobs.HazelcastJobService
import com.geekbeast.rhizome.jobs.JobStatus
import com.google.common.base.Stopwatch
import com.google.common.collect.ImmutableMap
import com.hazelcast.core.HazelcastInstance
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

const val DATA_DELETION_RATE = 30_000L

class BackgroundExpiredDataDeletionService(
        hazelcastInstance: HazelcastInstance,
        private val indexerConfiguration: IndexerConfiguration,
        private val auditingManager: AuditingManager,
        private val dataGraphService: DataGraphManager,
        private val deletionManager: DataDeletionManager,
        private val entitySetManager: EntitySetManager,
        private val jobService: HazelcastJobService
) {
    companion object {
        private val logger = LoggerFactory.getLogger(BackgroundExpiredDataDeletionService::class.java)!!
        private val NON_BLOCKING_JOB_STATUSES = EnumSet.of(JobStatus.FINISHED, JobStatus.CANCELED, JobStatus.PAUSED)
    }

    private val entitySets = HazelcastMap.ENTITY_SETS.getMap(hazelcastInstance)

    @Inject
    private lateinit var edm: EdmManager

    private val taskLock = ReentrantLock()

    @Suppress("UNUSED")
    @Scheduled(fixedRate = DATA_DELETION_RATE)
    fun deleteExpiredDataFromEntitySets() {
        if (!indexerConfiguration.backgroundExpiredDataDeletionEnabled) {
            logger.debug("Skipping expired data deletion as it is not enabled.")
            return
        }

        //Keep number of expired data deletion jobs under control
        if (!taskLock.tryLock()) {
            logger.debug("Not starting new expired data deletion job as an existing one is running.")
            return
        }

        logger.info("Starting background expired data deletion task.")
        try {
            val w = Stopwatch.createStarted()
            //We shuffle entity sets to make sure we have a chance to work share and index everything
            val totalDeleted = entitySets.values
                    .filter { it.hasExpirationPolicy() && !it.isLinking }
                    .shuffled()
                    .map {
                        try {
                            deleteExpiredData(it)
                        } catch (e: Exception) {
                            logger.error("An error occurred while trying to delete expired data for entity set {}", it.id, e)
                            0
                        }
                    }
                    .sum()

            logger.info(
                    "Completed deleting {} expired elements in {} ms.",
                    totalDeleted,
                    w.elapsed(TimeUnit.MILLISECONDS)
            )

        } finally {
            taskLock.unlock()
        }
    }

    private fun getBatchOfExpiringEkids(entitySet: EntitySet): MutableSet<UUID> {
        return dataGraphService.getExpiringEntitiesFromEntitySet(
                entitySet.id,
                entitySet.expiration!!,
                OffsetDateTime.now()
        ).toMutableSet()
    }

    private fun deleteExpiredData(entitySet: EntitySet): Int {
        logger.info(
                "Starting deletion of expired data for entity set {} with id {}",
                entitySet.name,
                entitySet.id
        )

        var totalDeletedEntitiesCount = 0
        var idsBatch = getBatchOfExpiringEkids(entitySet)

        while (idsBatch.isNotEmpty()) {
            val deletionJobId = deletionManager.clearOrDeleteEntities(
                    entitySet.id,
                    idsBatch,
                    entitySet.expiration!!.deleteType
            )

            blockUntilJobFinishedOrCanceled(deletionJobId)

            logger.info(
                    "Completed deleting {} expired elements from entity set {}.",
                    idsBatch.size,
                    entitySet.name
            )

            totalDeletedEntitiesCount += idsBatch.size

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

            idsBatch = getBatchOfExpiringEkids(entitySet)
        }

        return totalDeletedEntitiesCount
    }

    private fun blockUntilJobFinishedOrCanceled(jobId: UUID) {
        var status = jobService.getStatus(jobId)

        while (!NON_BLOCKING_JOB_STATUSES.contains(status)) {
            try {
                Thread.sleep(1000)
                status = jobService.getStatus(jobId)
            } catch (e: InterruptedException) {
                logger.error("Something bad happened while attempting to wait for job {} to complete.", jobId)
                return
            }
        }

        if (status != JobStatus.FINISHED) {
            logger.info("Terminating blocking for job {} as it is {}", jobId, status)
        }
    }
}