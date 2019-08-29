package com.openlattice.data

import com.fasterxml.jackson.annotation.JsonProperty
import com.openlattice.client.serialization.SerializationConstants
import com.openlattice.edm.set.ExpirationType
import java.util.*
import java.util.concurrent.TimeUnit

/**
 * Holds information about the expiration parameters of data within an entity set
 */

class DataExpiration (
        @JsonProperty( SerializationConstants.EXPIRATION ) timeToExpiration: TimeUnit,
        @JsonProperty( SerializationConstants.FLAGS_FIELD ) expirationFlag: ExpirationType,
        @JsonProperty( SerializationConstants.PROPERTY_TYPE_ID ) startDate: Optional<UUID> = Optional.empty()
) {
    val tTE = timeToExpiration
    val expFlag = expirationFlag
    val start = startDate
    init {
        check(expFlag == ExpirationType.DATE_PROPERTY && start.isEmpty) { "Must provide property type for expiration calculation" }
    }
}