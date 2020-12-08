package com.openlattice.data

import com.openlattice.analysis.requests.Filter
import java.util.*

data class FilteredDataPageDefinition(
        val propertyTypeId: UUID? = null,
        val filter: Filter? = null,
        val pageSize: Int = 10_000,
        val bookmarkId: UUID? = null
) {

    init {
        val filterFieldsPresent = propertyTypeId != null && filter != null
        val filterFieldsAbsent = propertyTypeId == null && filter == null
        check(filterFieldsPresent || filterFieldsAbsent) {
            "Cannot instantiate FilteredDataPageDefinition with propertyTypeId=$propertyTypeId and filter=$filter: " +
                    "one field cannot be null if the other is present."
        }
    }
}