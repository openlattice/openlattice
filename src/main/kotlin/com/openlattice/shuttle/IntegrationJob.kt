package com.openlattice.shuttle

import com.fasterxml.jackson.annotation.JsonProperty
import com.openlattice.client.serialization.SerializationConstants
import com.openlattice.mapstores.TestDataFactory

data class IntegrationJob(
        @JsonProperty(SerializationConstants.NAME) val integrationName: String,
        @JsonProperty(SerializationConstants.STATUS) var integrationStatus: IntegrationStatus
)