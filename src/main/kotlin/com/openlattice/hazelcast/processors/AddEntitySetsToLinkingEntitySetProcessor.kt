package com.openlattice.hazelcast.processors

import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.edm.EntitySet
import java.util.UUID


class AddEntitySetsToLinkingEntitySetProcessor(val entitySetIds: Set<UUID>) :
        AbstractRhizomeEntryProcessor<UUID, EntitySet, EntitySet>() {

    companion object {
        private const val serialVersionUID = 66023847982347L
    }

    override fun process(entry: MutableMap.MutableEntry<UUID, EntitySet>?): EntitySet {
        val entitySet = entry!!.value
        entitySet.linkedEntitySets.addAll(entitySetIds) // shouldn't be null at this point
        entry.setValue(entitySet)
        return entitySet
    }

    override fun hashCode(): Int {
        val prime = 53
        return prime + entitySetIds.hashCode()
    }

    override fun equals(other: Any?): Boolean {
        if (other == null) return false
        if (this::class.java != other::class.java) return false

        val otherProcessor = other as (AddEntitySetsToLinkingEntitySetProcessor)
        if (entitySetIds != otherProcessor.entitySetIds) return false
        return true
    }
}