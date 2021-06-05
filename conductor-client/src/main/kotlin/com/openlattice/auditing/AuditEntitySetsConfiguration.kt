package com.openlattice.auditing

import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
data class AuditEntitySetsConfiguration(
        val auditRecordEntitySet: UUID?,
        val auditEdgeEntitySet: UUID?
)