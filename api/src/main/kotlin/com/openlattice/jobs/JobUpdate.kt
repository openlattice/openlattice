package com.openlattice.jobs

import com.geekbeast.rhizome.jobs.JobStatus

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
data class JobUpdate(
        val status: JobStatus,
        val reload: Boolean
)