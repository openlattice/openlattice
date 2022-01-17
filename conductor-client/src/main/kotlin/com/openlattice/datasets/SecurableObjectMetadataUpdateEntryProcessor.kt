package com.openlattice.datasets

import com.geekbeast.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.authorization.AclKey

data class SecurableObjectMetadataUpdateEntryProcessor(
        val update: SecurableObjectMetadataUpdate
) : AbstractRhizomeEntryProcessor<AclKey, SecurableObjectMetadata?, Boolean>() {

    override fun process(entry: MutableMap.MutableEntry<AclKey, SecurableObjectMetadata?>): Boolean {
        val value = entry.value ?: return false

        update.title?.let { value.title = it }
        update.description?.let { value.description = it }
        update.contacts?.let { value.contacts = it }
        update.flags?.let { value.flags = it }
        update.metadata?.let { value.metadata = it }

        entry.setValue(value)
        return true
    }
}