package com.openlattice.search

import com.fasterxml.jackson.annotation.JsonProperty
import com.google.common.base.Preconditions
import com.openlattice.client.serialization.SerializationConstants
import java.util.*

data class SortDefinition(
        @JsonProperty(SerializationConstants.TYPE_FIELD) val sortType: SortType = SortType.score,
        @JsonProperty(SerializationConstants.IS_DESCENDING) val isDescending: Boolean = true,
        @JsonProperty(SerializationConstants.PROPERTY_TYPE_ID) val propertyTypeId: Optional<UUID> = Optional.empty(),
        @JsonProperty(SerializationConstants.LATITUDE) val latitude: Optional<Double> = Optional.empty(),
        @JsonProperty(SerializationConstants.LONGITUDE) val longitude: Optional<Double> = Optional.empty()
) {

    init {

        when (sortType) {

            SortType.field -> Preconditions.checkNotNull(
                    propertyTypeId,
                    "The sort type field requires a propertyTypeId to be present.")

            SortType.geoDistance -> {
                Preconditions.checkArgument(
                        propertyTypeId.isPresent,
                        "The sort type geoDistance requires a propertyTypeId to be present.")
                Preconditions.checkArgument(
                        latitude.isPresent,
                        "The sort type geoDistance requires a latitude to be present.")
                Preconditions.checkArgument(
                        longitude.isPresent,
                        "The sort type geoDistance requires a longitude to be present.")
            }
        }
    }
}