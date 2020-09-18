package com.openlattice.data.ids.jobs

import com.geekbeast.rhizome.jobs.JobState
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
data class FixDuplicateIdAssignmentJobState( var id: UUID) : JobState