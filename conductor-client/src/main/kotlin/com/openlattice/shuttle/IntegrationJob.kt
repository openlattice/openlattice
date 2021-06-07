package com.openlattice.shuttle

import com.fasterxml.jackson.annotation.JsonProperty
import com.openlattice.client.serialization.SerializationConstants

data class IntegrationJob(
        @JsonProperty(SerializationConstants.NAME) val integrationName: String,
        @JsonProperty(SerializationConstants.STATUS) var integrationStatus: IntegrationStatus
)