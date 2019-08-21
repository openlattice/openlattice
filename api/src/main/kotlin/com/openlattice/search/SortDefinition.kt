package com.openlattice.search

import com.fasterxml.jackson.annotation.JsonProperty
import com.google.common.base.Preconditions
import com.openlattice.client.serialization.SerializationConstants
import java.util.*

data class SortDefinition(
        @JsonProperty(SerializationConstants.TYPE_FIELD) val sortType: SortType = SortType.score,
        @JsonProperty(SerializationConstants.IS_DESCENDING) val isDescending: Boolean = true,
        @JsonProperty(SerializationConstants.PROPERTY_TYPE_ID) val propertyTypeId: UUID? = null,
        @JsonProperty(SerializationConstants.LATITUDE) val latitude: Double? = null,
        @JsonProperty(SerializationConstants.LONGITUDE) val longitude: Double? = null
) {

    init {

        when (sortType) {

            SortType.field -> Preconditions.checkNotNull(
                    propertyTypeId,
                    "The sort type field requires a propertyTypeId to be present.")

            SortType.geoDistance -> {
                Preconditions.checkNotNull(
                        propertyTypeId,
                        "The sort type geoDistance requires a propertyTypeId to be present.")
                Preconditions.checkNotNull(
                        latitude,
                        "The sort type geoDistance requires a latitude to be present.")
                Preconditions.checkNotNull(
                        longitude,
                        "The sort type geoDistance requires a longitude to be present.")
            }
        }
    }
}