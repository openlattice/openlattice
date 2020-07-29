package com.openlattice.graph.partioning

import com.geekbeast.rhizome.jobs.AbstractDistributedJob
import com.geekbeast.rhizome.jobs.DistributedJobState

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class RepartitioningJob(state: DistributedJobState) : AbstractDistributedJob<Long>(state) {
    private var
    override fun result(): Long? {
        TODO("Not yet implemented")
    }

    override fun next() {
        /**
         * Phase 1
         * Do a INSERT INTO ... SELECT FROM to re-partition the data.
         *
         * Loads by ID will temporarily fail, until data has been inserted to the appropriate partition.
         *
         * Writes will likely be blocked
         */

        /**
         * Phase 2
         * Delete data whose partition doesn't match it's computed partition. 
         */
    }

    override fun hasWorkRemaining(): Boolean {
        (state as RepartitioningJobState).
    }
}