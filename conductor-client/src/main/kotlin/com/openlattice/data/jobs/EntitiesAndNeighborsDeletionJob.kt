package com.openlattice.data.jobs

import com.fasterxml.jackson.annotation.JsonCreator
import com.geekbeast.rhizome.jobs.AbstractDistributedJob
import com.geekbeast.rhizome.jobs.JobStatus
import com.openlattice.graph.PagedNeighborRequest
import com.openlattice.ioc.providers.LateInitAware
import com.openlattice.ioc.providers.LateInitProvider
import java.util.*

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
class EntitiesAndNeighborsDeletionJob(
        state: EntitiesAndNeighborsDeletionJobState,
) : AbstractDistributedJob<Long, EntitiesAndNeighborsDeletionJobState>(state), LateInitAware {

    @JsonCreator
    constructor(
            id: UUID?,
            taskId: Long?,
            status: JobStatus,
            progress: Byte,
            hasWorkRemaining: Boolean,
            result: Long?,
            state: EntitiesAndNeighborsDeletionJobState
    ) : this(state) {
        initialize(id, taskId, status, progress, hasWorkRemaining, result)
    }

    companion object {
        private const val STATUS_CHECK_INTERVAL = 5000L // 5 seconds
    }

    override val resumable = true

    @Transient
    private lateinit var lateInitProvider: LateInitProvider

    override fun initialize() {
        val neighborEntityKeyIds = getNeighborEntityKeyIds()
        state.entitySetIdEntityKeyIds += neighborEntityKeyIds
        publishJobState()
    }

    /**
     * Take an entity set id and associated entity key ids from the map and submit a DataDeletionJob
     */
    override fun processNextBatch() {
        val entitySetId = getNextEntitySetId()
        if (entitySetId == null) {
            logger.info("No more delete jobs to submit for processing")
            if (allDeleteJobsFinished()) {
                hasWorkRemaining = false
                status = JobStatus.FINISHED
                updateJobStatus()
                publishJobState()
                logger.info("finished deleting entities and neighbors of entity set ${state.entitySetId}")
            }
            Thread.sleep(STATUS_CHECK_INTERVAL)
            return
        }

        val entityKeyIds = state.entitySetIdEntityKeyIds.getValue(entitySetId)

        /**
         *  safety check to avoid deleting all entities from current entity set
         *  At initialization, all entity set ids are mapped to a non-empty set of entity key ids, so ideally we should
         *  never end up at this state
         */
        if (entityKeyIds.isEmpty()) {
            logger.warn("skipping entity set $entitySetId since there are no entities to delete")
            cleanUpBatch(entitySetId)
            publishJobState()
            return
        }

        val jobService = lateInitProvider.dataGraphService.getJobService()

        val jobId = jobService.submitJob(DataDeletionJob(DataDeletionJobState(
                entitySetId,
                state.deleteType,
                state.partitions.getValue(entitySetId),
                entityKeyIds.toMutableSet()
        )))
        state.dataDeletionJobIds.add(jobId)

        logger.info("submitted delete job $jobId for ${entityKeyIds.size} entities in entity set $entitySetId")
        cleanUpBatch(entitySetId)
        publishJobState()
    }

    private fun allDeleteJobsFinished(): Boolean {
        val jobService = lateInitProvider.dataGraphService.getJobService()
        val statuses = state.dataDeletionJobIds.map { jobService.getStatus(it) }

        return statuses.all { it == JobStatus.FINISHED }
    }

    private fun cleanUpBatch(entitySetId: UUID) {
        state.entitySetIdEntityKeyIds.remove(entitySetId)
    }

    private fun getNextEntitySetId(): UUID? {
        val entitySetIds = state.entitySetIdEntityKeyIds.keys

        return if (entitySetIds.isEmpty()) null else entitySetIds.first()
    }


    private fun getNeighborEntityKeyIds(): Map<UUID, Set<UUID>> {

        val graphService = lateInitProvider.dataGraphService.getGraphService()
        val entityKeyIds = state.filter.entityKeyIds

        val neighbors = mutableMapOf<UUID, MutableSet<UUID>>()

        graphService.getEdgesAndNeighborsForVertices(setOf(state.entitySetId), PagedNeighborRequest(state.filter)).forEach {
            val isSrc = entityKeyIds.contains(it.src.entityKeyId)

            val neighborEntityDataKey = if (isSrc) it.dst else it.src
            val entitySetId = neighborEntityDataKey.entitySetId
            val entityKeyId = neighborEntityDataKey.entityKeyId

            neighbors.computeIfAbsent(entitySetId) { mutableSetOf() }.add(entityKeyId)
        }

        return neighbors
    }

    override fun setLateInitProvider(lateInitProvider: LateInitProvider) {
        this.lateInitProvider = lateInitProvider
    }
}