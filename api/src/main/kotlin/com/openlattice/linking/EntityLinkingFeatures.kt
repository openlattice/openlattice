package com.openlattice.linking

import com.fasterxml.jackson.annotation.JsonProperty
import com.openlattice.client.serialization.SerializationConstants

/**
 * Represents the manual feedback along with extracted features given between two entities, whether they are supposed to
 * be linked or not.
 */
data class EntityLinkingFeatures(
        @JsonProperty(SerializationConstants.LINKING_FEEDBACK) val entityLinkingFeedback: EntityLinkingFeedback,
        @JsonProperty(SerializationConstants.FEATURES) val features: Map<String, Double>
)