package com.openlattice.data.jobs

import com.geekbeast.rhizome.jobs.AbstractDistributedJob
import com.geekbeast.rhizome.jobs.HazelcastJobService
import com.geekbeast.rhizome.jobs.JobStatus
import com.geekbeast.util.log
import com.openlattice.graph.PagedNeighborRequest
import com.openlattice.graph.core.GraphService
import com.openlattice.ioc.providers.LateInitAware
import com.openlattice.ioc.providers.LateInitProvider
import java.util.*
import kotlin.streams.toList

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
class EntitiesAndNeighborsDeletionJob(
        state: EntitiesAndNeighborsDeletionJobState,
) : AbstractDistributedJob<Long, EntitiesAndNeighborsDeletionJobState>(state), LateInitAware {

    override val resumable = true

    @Transient
    private lateinit var lateInitProvider: LateInitProvider

    override fun initialize() {
        val neighborEntityKeyIds = getNeighborEntityKeyIds()
        state.entitySetIdEntityKeyIds += neighborEntityKeyIds
        publishJobState()
    }


    override fun processNextBatch() {
        val entitySetId = getNextEntitySetId()
        if (entitySetId == null) {
            logger.info("no more delete jobs to submit for processing")
            if (allDeleteJobsFinished()) {
                hasWorkRemaining = false
                status = JobStatus.FINISHED
                updateJobStatus()
                publishJobState()
            }
            return
        }

        // safety measure to avoid deleting all entities from current entity set
        if (state.entitySetIdEntityKeyIds.getValue(entitySetId).isEmpty()) {
            logger.info("No entities to delete for entity set $entitySetId")
            return
        }

        val jobService = lateInitProvider.dataGraphService.getJobService()

        val jobId = jobService.submitJob(DataDeletionJob(DataDeletionJobState(
                entitySetId,
                state.deleteType,
                state.partitions.getValue(entitySetId),
                state.entitySetIdEntityKeyIds[entitySetId]?.toMutableSet()
        )))
        state.dataDeletionJobIds.add(jobId)

        logger.info("submitted delete job $jobId for entitySet: $entitySetId")
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

        val edges =  graphService.getEdgesAndNeighborsForVertices(setOf(state.entitySetId), PagedNeighborRequest(state.filter))
        return  edges
                .toList()
                .groupBy { it.edge.entitySetId }
                .mapValues { it -> it.value.mapNotNull { it.edge.entityKeyId }.toSet() }
    }

    override fun setLateInitProvider(lateInitProvider: LateInitProvider) {
        this.lateInitProvider = lateInitProvider
    }
}