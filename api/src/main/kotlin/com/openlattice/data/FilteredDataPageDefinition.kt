package com.openlattice.data

import com.openlattice.analysis.requests.Filter
import java.util.*

data class FilteredDataPageDefinition(
        val propertyTypeId: UUID,
        val filter: Filter,
        val pageSize: Int = 10_000,
        val bookmarkId: UUID? = null
)