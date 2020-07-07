package com.openlattice.ids

import com.geekbeast.hazelcast.HazelcastClientProvider
import com.geekbeast.hazelcast.IHazelcastClientProvider
import com.google.common.collect.Queues
import com.google.common.util.concurrent.ListeningExecutorService
import com.openlattice.hazelcast.HazelcastClient
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.hazelcast.HazelcastQueue
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.BlockingQueue
import java.util.concurrent.Executors

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class HazelcastIdGenerationService(clients: IHazelcastClientProvider) {

    /*
     * This should be good enough until we scale past 65536 Hazelcast nodes.
     */
    companion object {
        private const val PARTITION_SCROLL_SIZE = 5
        private const val MASK_LENGTH = 16
        const val NUM_PARTITIONS = 1 shl MASK_LENGTH //65536
        private val logger = LoggerFactory.getLogger(HazelcastIdGenerationService::class.java)
        private val executor = Executors.newSingleThreadExecutor()
    }

    /*
     * Each range owns a portion of the keyspace.
     */
    private val hazelcastInstance = clients.getClient(HazelcastClient.IDS.name)
    private val scrolls = HazelcastMap.ID_GENERATION.getMap(hazelcastInstance)
    private val idsQueue = HazelcastQueue.ID_GENERATION.getQueue(hazelcastInstance)
    private val localQueue = Queues.newArrayBlockingQueue<UUID>(NUM_PARTITIONS) as BlockingQueue<UUID>

    init {
        if (scrolls.isEmpty) {
            //Initialize the ranges
            scrolls.putAll((0L until NUM_PARTITIONS).associateWith { Range(it shl 48) })
        }
    }

    private val enqueueJob = executor.execute {
        while (true) {
            val ids = try {
                //Use the 0 key a fence around the entire map.
                scrolls.lock(0L)
                //TODO: Handle exhaustion of partition.
                scrolls.executeOnEntries(IdsGeneratingEntryProcessor(PARTITION_SCROLL_SIZE)) as Map<Long, List<UUID>>
            } finally {
                scrolls.unlock(0L)
            }

            ids.values.asSequence().flatten().forEach { idsQueue.put(it) }

            logger.info("Added $NUM_PARTITIONS ids to queue")
        }
    }

    /**
     * Returns an id to the local id for later use.
     * @param id to return to the pool
     */
    fun returnId(id: UUID) {
        localQueue.offer(id)
    }

    fun returnIds(ids: Collection<UUID>) {
        ids.forEach(::returnId)
    }

    fun getNextIds(count: Int): Set<UUID> {
        return generateSequence { getNextId() }.take(count).toSet()
    }

    fun getNextId(): UUID {
        return localQueue.poll() ?: idsQueue.take()
    }
}