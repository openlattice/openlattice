package com.openlattice.auditing

import com.amazonaws.services.s3.model.ListObjectsV2Request
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.model.PutObjectRequest
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.google.common.collect.Queues
import com.google.common.util.concurrent.MoreExecutors
import com.openlattice.aws.newS3Client
import com.openlattice.ids.HazelcastLongIdService
import com.openlattice.ids.IdScopes
import org.springframework.http.MediaType
import java.util.concurrent.Executors

private const val LONG_IDS_BATCH_SIZE = 8192

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */

class S3AuditingService(
        auditingConfiguration: AuditingConfiguration,
        private val longIdService: HazelcastLongIdService,
        private val mapper: ObjectMapper

) : AuditingManager {

    init {
        if (auditingConfiguration.awsS3ClientConfiguration.isEmpty) {
            throw IllegalStateException("Auditing configuration from auditing.yaml must include S3 configuration details.")
        }
        if (!auditingConfiguration.enabled) {
            throw IllegalStateException("Auditing configuration for s3 should not be disabled")
        }
    }

    private val partitions = auditingConfiguration.partitions
    private val bucket = auditingConfiguration.awsS3ClientConfiguration.get().bucket
    private val longIdsQueue = Queues.newArrayBlockingQueue<Long>(LONG_IDS_BATCH_SIZE)
    private val executorService = MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor())

    private val s3 = newS3Client(auditingConfiguration.awsS3ClientConfiguration.get())

    private val refreshQueue = executorService.execute {
        while (true) {
            longIdService
                    .getIds(IdScopes.AUDITING.name, LONG_IDS_BATCH_SIZE.toLong())
                    .forEach(longIdsQueue::put)
        }
    }

    override fun recordEvents(events: List<AuditableEvent>): Int {
        val eventsBytes = mapper.writeValueAsBytes(events)
        val eventsInputStream = eventsBytes.inputStream()

        val metadata = ObjectMetadata()
        metadata.contentLength = eventsInputStream.available().toLong()
        metadata.contentType = MediaType.APPLICATION_JSON_VALUE

        val id = getId()
        val partition = id % partitions.toLong()
        val key = "$partition/${System.currentTimeMillis()}/event-$id.json"
        s3.putObject(PutObjectRequest(bucket, key, eventsInputStream, metadata))
        return events.size
    }

    fun getRecordedEvents(): List<AuditableEvent> {
        //TODO: Need to make sure that auditable event is only queued up once from s3


        val objectListing = s3.listObjectsV2(ListObjectsV2Request().withBucketName(bucket))

        return objectListing.objectSummaries.flatMap { objSummary ->
            return@flatMap mapper.readValue<List<AuditableEvent>>(s3.getObject(bucket, objSummary.key).objectContent)
        }
    }

    fun deleteIntegratedEvents(keys: List<String>) {
        TODO("Implement deleting events after they are stored.")
    }

    private fun getId(): Long {
        return longIdsQueue.take()

    }

}