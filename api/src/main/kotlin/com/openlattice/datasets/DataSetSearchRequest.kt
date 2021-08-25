package com.openlattice.datasets

import com.openlattice.search.requests.ConstraintGroup
import java.util.*

data class DataSetSearchRequest(
    val constraints: List<ConstraintGroup>,
    val maxHits: Int = 10_000,
    val organizationIds: Set<UUID> = emptySet(),
    val start: Int = 0
)
