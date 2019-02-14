package com.openlattice.linking

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonProperty
import com.openlattice.client.serialization.SerializationConstants
import com.openlattice.data.EntityDataKey
import java.util.UUID

/**
 * Represents an ordered pair of EntityDataKeys
 */
class EntityKeyPair @JsonCreator constructor(
        @JsonProperty(SerializationConstants.FIRST) first: EntityDataKey,
        @JsonProperty(SerializationConstants.SECOND) second: EntityDataKey) {
    @JsonProperty(SerializationConstants.FIRST) var first: EntityDataKey
    @JsonProperty(SerializationConstants.SECOND) var second: EntityDataKey

    init {
        val entityPair = sortedSetOf(first, second)
        this.first = entityPair.first()
        this.second = entityPair.last()
    }

    override fun equals(other: Any?): Boolean {
        if (other == null) return false
        if (other !is EntityKeyPair) return false
        if (other.first != first) return false
        if (other.second != second) return false

        return true
    }

    override fun hashCode(): Int {
        return first.hashCode() * 31 + second.hashCode()
    }

    override fun toString(): String {
        return "EntityKeyPair($first, $second)"
    }

    /**
     * Acessor functions for LinkingFeedbackMapstore
     */
    @JsonIgnore
    fun getFirstEntitySetId(): UUID {
        return first.entitySetId
    }

    @JsonIgnore
    fun getFirstEntityKeyId(): UUID {
        return first.entityKeyId
    }

    @JsonIgnore
    fun getSecondEntitySetId(): UUID {
        return second.entitySetId
    }

    @JsonIgnore
    fun getSecondEntityKeyId(): UUID {
        return second.entityKeyId
    }
}