package com.openlattice.linking

import com.fasterxml.jackson.annotation.JsonProperty
import com.openlattice.client.serialization.SerializationConstants
import com.openlattice.data.EntityDataKey

/**
 * Represents the manual feedback given between two entities, whether they are supposed to be linked or not
 */
data class EntityLinkingFeedback(
        @JsonProperty(SerializationConstants.SRC) val src: EntityDataKey,
        @JsonProperty(SerializationConstants.DST) val dst: EntityDataKey,
        @JsonProperty(SerializationConstants.LINKED) val linked: Boolean
)