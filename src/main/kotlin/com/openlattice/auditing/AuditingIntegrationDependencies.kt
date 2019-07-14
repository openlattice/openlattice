package com.openlattice.auditing

import com.fasterxml.jackson.databind.ObjectMapper
import com.openlattice.data.DataGraphManager
import com.openlattice.tasks.HazelcastTaskDependencies

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class AuditingIntegrationDependencies(
        val dataGraphManager: DataGraphManager,
        val auditRecordEntitySetsManager: AuditRecordEntitySetsManager,
        val s3AuditingQueue: S3AuditingQueue,
        val mapper: ObjectMapper
) : HazelcastTaskDependencies