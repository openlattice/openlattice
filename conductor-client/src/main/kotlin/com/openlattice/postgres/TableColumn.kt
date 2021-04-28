package com.openlattice.postgres

import com.openlattice.postgres.external.Schemas
import java.util.*

/**
 * @author Drew Bailey (drew@openlattice.com)
 */
data class TableColumn(
        val organizationId: UUID,
        val tableId: UUID,
        val columnId: UUID,
        val schema: Schemas,
        val name: String,
        val tableName: String
)