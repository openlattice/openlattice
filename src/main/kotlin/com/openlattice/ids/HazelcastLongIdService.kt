package com.openlattice.ids

import com.geekbeast.hazelcast.HazelcastClientProvider
import com.hazelcast.core.HazelcastInstance
import com.openlattice.hazelcast.HazelcastClient
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.ids.processors.LongIdsGeneratingProcessor

/**
 * Used for generating scoped long ids.
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class HazelcastLongIdService(hazelcastClientProvider: HazelcastClientProvider, hazelcastInstance: HazelcastInstance) {
    private val counters = hazelcastClientProvider
            .getClient(HazelcastClient.IDS.name)
            .getMap<String, Long>(HazelcastMap.LONG_IDS.name)

    /**
     * Generates a unique id of [scope]
     * @param scope The scope to use for id generation.
     * @return A unique long id (subject to hazelcast split brain mechanics).
     */
    fun getId( scope: String ) : Long {
        return getIds( scope, 1 ).first
    }

    /**
     * Generates a unique id of [scope]
     * @param scope The scope to use for id generation.
     * @param count The number of ids to generate.
     * @return A unique long id (subject to hazelcast split brain mechanics).
     */
    fun getIds( scope: String , count : Long ) : LongRange {
        val base = counters.executeOnKey(scope, LongIdsGeneratingProcessor(count)) as Long
        return base until (base+count)
    }
}