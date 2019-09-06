package com.openlattice.data

import com.fasterxml.jackson.annotation.JsonProperty
import com.openlattice.client.serialization.SerializationConstants
import com.openlattice.edm.set.ExpirationType
import java.util.*
import java.util.concurrent.TimeUnit

/**
 * Holds information about the expiration parameters of data within an entity set
 */

data class DataExpiration(
        @JsonProperty(SerializationConstants.EXPIRATION) val timeToExpiration: Long,
        @JsonProperty(SerializationConstants.FLAGS_FIELD) val expirationFlag: ExpirationType,
        @JsonProperty(SerializationConstants.PROPERTY_TYPE_ID) val startDateProperty: Optional<UUID> = Optional.empty()
) {

    init {
        if (expirationFlag == ExpirationType.DATE_PROPERTY) check(startDateProperty.isPresent) { "Must provide property type for expiration calculation" }
    }
}