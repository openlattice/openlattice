package com.openlattice.linking

import com.fasterxml.jackson.annotation.JsonProperty
import com.openlattice.client.serialization.SerializationConstants

data class MatchedEntityPair(
        @JsonProperty(SerializationConstants.ENTITY_KEY_IDS) val entityPair: EntityKeyPair,
        @JsonProperty(SerializationConstants.MATCH) val match: Double) {

    override fun equals(other: Any?): Boolean {
        if(other == null) return false
        if(other !is MatchedEntityPair) return false
        return entityPair == other.entityPair
    }
}