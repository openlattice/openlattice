package com.openlattice.collections.processors

import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.edm.collection.EntitySetCollection
import com.openlattice.edm.requests.MetadataUpdate
import java.util.*

class UpdateEntitySetCollectionMetadataProcessor(val update: MetadataUpdate):
        AbstractRhizomeEntryProcessor<UUID, EntitySetCollection, EntitySetCollection?>() {

    override fun process(entry: MutableMap.MutableEntry<UUID, EntitySetCollection>?): EntitySetCollection? {
        val collection = entry?.value ?: return null

        if (update.title.isPresent) {
            collection.title = update.title.get()
        }

        if (update.description.isPresent) {
            collection.description = update.description.get()
        }

        if (update.name.isPresent) {
            collection.name = update.name.get()
        }

        if (update.contacts.isPresent) {
            collection.contacts = update.contacts.get()
        }

        if (update.organizationId.isPresent) {
            collection.organizationId = update.organizationId.get()
        }

        entry.setValue(collection)
        return null
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as UpdateEntitySetCollectionMetadataProcessor

        if (update != other.update) return false

        return true
    }

    override fun hashCode(): Int {
        return update.hashCode()
    }


}