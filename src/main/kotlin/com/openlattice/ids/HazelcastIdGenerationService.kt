package com.openlattice.ids

import com.geekbeast.hazelcast.HazelcastClientProvider
import com.google.common.util.concurrent.ListeningExecutorService
import com.openlattice.hazelcast.HazelcastClient
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.hazelcast.HazelcastQueue
import org.slf4j.LoggerFactory
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class HazelcastIdGenerationService(clients: HazelcastClientProvider, private val executor: ListeningExecutorService) {

    /*
     * This should be good enough until we scale past 65536 Hazelcast nodes.
     */
    companion object {
        private const val MASK_LENGTH = 16
        const val NUM_PARTITIONS = 1 shl MASK_LENGTH //65536
        private val logger = LoggerFactory.getLogger(HazelcastIdGenerationService::class.java)
    }

    /*
     * Each range owns a portion of the keyspace.
     */
    private val hazelcastInstance = clients.getClient(HazelcastClient.IDS.name)
    private val scrolls = HazelcastMap.ID_GENERATION.getMap( hazelcastInstance )
    private val idsQueue = HazelcastQueue.ID_GENERATION.getQueue( hazelcastInstance )

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
                scrolls.executeOnEntries(IdsGeneratingEntryProcessor(1))
            } finally {
                scrolls.unlock(0L)
            }

            ids.values
                    .map { (it as List<UUID>)[0] }
                    .forEach { idsQueue.put(it) }
            logger.info("Added $NUM_PARTITIONS ids to queue")
        }
    }

    fun returnId( id: UUID ) {
        executor.submit {
            idsQueue.put(id)
        }
    }

    fun getNextIds(count: Int): Set<UUID> {
        return generateSequence { idsQueue.take() }.take(count).toSet()
    }

    fun getNextId(): UUID {
        return idsQueue.take()
    }
}