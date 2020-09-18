package com.openlattice.data.ids.jobs

import com.geekbeast.rhizome.jobs.AbstractDistributedJob
import com.openlattice.hazelcast.serializers.decorators.MetastoreAware
import com.zaxxer.hikari.HikariDataSource

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class FixDuplicateIdAssignmentJob(
        state: FixDuplicateIdAssignmentJobState
): AbstractDistributedJob<Long, FixDuplicateIdAssignmentJobState>(state), MetastoreAware {
    @Transient
    private lateinit var hds: HikariDataSource

    override fun processNextBatch() {
        /**
         * Process ids in batches
         */
    }

    override fun setHikariDataSource(hds: HikariDataSource) {
        this.hds = hds
    }

}