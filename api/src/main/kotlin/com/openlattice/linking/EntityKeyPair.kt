package com.openlattice.linking

import com.fasterxml.jackson.annotation.JsonProperty
import com.openlattice.client.serialization.SerializationConstants
import com.openlattice.data.EntityDataKey

/**
 * Represents an ordered pair of EntityDataKeys
 */
class EntityKeyPair(
        @JsonProperty(SerializationConstants.FIRST) first: EntityDataKey,
        @JsonProperty(SerializationConstants.SECOND) second: EntityDataKey) {

    companion object {
        val entityKeyComparator = Comparator<EntityDataKey> { key1, key2 ->
            if (key1.entitySetId != key2.entitySetId) {
                key1.entitySetId.compareTo(key2.entitySetId)
            } else {
                key1.entityKeyId.compareTo(key2.entityKeyId)
            }
        }
    }

    private val entityPair: Set<EntityDataKey> = sortedSetOf(entityKeyComparator, first, second)

    override fun equals(other: Any?): Boolean {
        if (other == null) return false
        if (other !is EntityKeyPair) return false
        if (other.entityPair != entityPair) return false

        return true
    }

    override fun hashCode(): Int {
        return entityPair.hashCode()
    }

    override fun toString(): String {
        return "EntityPair(${getFirst()}, ${getSecond()})"
    }

    @JsonProperty(SerializationConstants.FIRST)
    fun getFirst(): EntityDataKey {
        return entityPair.first()
    }

    @JsonProperty(SerializationConstants.SECOND)
    fun getSecond(): EntityDataKey {
        return entityPair.last()
    }
}