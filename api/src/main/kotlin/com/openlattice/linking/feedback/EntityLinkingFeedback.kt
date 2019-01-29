package com.openlattice.linking.feedback

import com.fasterxml.jackson.annotation.JsonProperty
import com.openlattice.client.serialization.SerializationConstants
import com.openlattice.data.EntityDataKey

data class EntityLinkingFeedback(
        @JsonProperty(SerializationConstants.SRC) val src: EntityDataKey,
        @JsonProperty(SerializationConstants.DST) val dst: EntityDataKey,
        @JsonProperty(SerializationConstants.LINKED) val linked: Boolean
)