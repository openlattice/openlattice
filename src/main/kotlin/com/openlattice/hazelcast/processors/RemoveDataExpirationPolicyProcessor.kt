package com.openlattice.hazelcast.processors

import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.edm.EntitySet
import java.util.UUID

class RemoveDataExpirationPolicyProcessor() :
        AbstractRhizomeEntryProcessor<UUID, EntitySet, EntitySet>() {

    companion object {
        private const val serialVersionUID = -6602384557982347L
    }

    override fun process(entry: MutableMap.MutableEntry<UUID, EntitySet?>): EntitySet? {
        val entitySet = entry.value!!
        entitySet.expiration = null
        entry.setValue(entitySet)
        return entitySet
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false
        return true
    }

    override fun hashCode(): Int {
        return javaClass.hashCode()
    }

}