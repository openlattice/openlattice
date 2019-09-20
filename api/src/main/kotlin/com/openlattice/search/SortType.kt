package com.openlattice.search

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind

private val ALLOWED_FIELD_DATATYPES = setOf(
        /* dates and times */
        EdmPrimitiveTypeKind.Date,
        EdmPrimitiveTypeKind.DateTimeOffset,
        EdmPrimitiveTypeKind.TimeOfDay,

        /* numbers */
        EdmPrimitiveTypeKind.Int16,
        EdmPrimitiveTypeKind.Int32,
        EdmPrimitiveTypeKind.Int64,
        EdmPrimitiveTypeKind.Decimal,
        EdmPrimitiveTypeKind.Double,
        EdmPrimitiveTypeKind.Duration
)

private val ALLOWED_GEO_DATATYPES = setOf(
        EdmPrimitiveTypeKind.GeographyPoint
)

private val ALLOWED_SCORE_DATATYPES = EdmPrimitiveTypeKind.values().toSet()

enum class SortType(val allowedDatatypes: Set<EdmPrimitiveTypeKind>) {
    field(ALLOWED_FIELD_DATATYPES),
    score(ALLOWED_SCORE_DATATYPES),
    geoDistance(ALLOWED_GEO_DATATYPES);
}


