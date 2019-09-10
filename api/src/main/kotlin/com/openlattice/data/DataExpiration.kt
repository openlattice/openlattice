package com.openlattice.data

import com.fasterxml.jackson.annotation.JsonProperty
import com.openlattice.client.serialization.SerializationConstants
import com.openlattice.edm.set.ExpirationBase
import java.util.*

/**
 * Holds information about the expiration parameters of data within an entity set.
 *
 * @param timeToExpiration holds the duration in ms that data will live before expiring.
 *
 * @param expirationBase is a flag for what will be used as a base date to calculate
 * expiration date. It may be an entity's first write, last write, or a datetime
 * property type within the entity. Thus, an entity's expiration datetime is calculated
 * as expirationBase + timeToExpire.
 *
 * @param startDateProperty If the expirationBase is a datetime property type, startDateProperty will hold the
 * UUID of that property type. Otherwise, it will remain empty.
 */

data class DataExpiration(
        @JsonProperty(SerializationConstants.EXPIRATION) val timeToExpiration: Long,
        @JsonProperty(SerializationConstants.FLAGS_FIELD) val expirationBase: ExpirationBase,
        @JsonProperty(SerializationConstants.PROPERTY_TYPE_ID) val startDateProperty: Optional<UUID> = Optional.empty()
) {

    init {
        if (expirationBase == ExpirationBase.DATE_PROPERTY) check(startDateProperty.isPresent) { "Must provide property type for expiration calculation" }
    }
}