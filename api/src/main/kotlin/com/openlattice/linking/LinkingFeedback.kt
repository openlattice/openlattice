package com.openlattice.linking

import com.fasterxml.jackson.annotation.JsonProperty
import com.openlattice.client.serialization.SerializationConstants
import com.openlattice.data.EntityDataKey

/**
 * Represents the manual feedback given on linked entities with the same linking id within a linking entity set.
 */
data class LinkingFeedback(
        @JsonProperty(SerializationConstants.LINKING_ENTITY) val linkingEntityDataKey: EntityDataKey,
        @JsonProperty(SerializationConstants.LINKING_ENTITIES) val linkingEntities: Set<EntityDataKey>,
        @JsonProperty(SerializationConstants.NON_LINKING_ENTITIES) val nonLinkingEntities: Set<EntityDataKey>
)