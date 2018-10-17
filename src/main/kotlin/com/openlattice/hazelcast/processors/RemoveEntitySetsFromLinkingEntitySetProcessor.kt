package com.openlattice.hazelcast.processors

import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.edm.EntitySet
import java.util.UUID

class RemoveEntitySetsFromLinkingEntitySetProcessor( val entitySetIds : Set<UUID>):
        AbstractRhizomeEntryProcessor<UUID, EntitySet, Void>() {

    companion object {
        private const val serialVersionUID = -6602384557982347L
    }

    override fun process(entry: MutableMap.MutableEntry<UUID, EntitySet>?): Void? {
        val entitySet = entry!!.value
        entitySet.linkedEntitySets.removeAll( entitySetIds ) // shouldn't be null at this point
        entry.setValue( entitySet )
        return null
    }
}