package com.openlattice.data.integration

import com.fasterxml.jackson.annotation.JsonProperty
import java.util.*

data class S3EntityData(
        @JsonProperty val entitySetId: UUID,
        @JsonProperty val entityKeyId: UUID,
        @JsonProperty val propertyTypeId: UUID,
        @JsonProperty val propertyHash: String
)