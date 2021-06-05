package com.openlattice.shuttle

import com.fasterxml.jackson.annotation.JsonProperty
import com.openlattice.client.serialization.SerializationConstants
import java.util.*

data class QueuedIntegrationJob(
        @JsonProperty(SerializationConstants.ID_FIELD) val jobId: UUID,
        @JsonProperty(SerializationConstants.JOB) val integrationJob: IntegrationJob
)