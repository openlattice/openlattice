package com.openlattice.datasets

import com.openlattice.edm.requests.MetadataUpdate

data class SecurableObjectMetadataUpdate(
        val title: String? = null,
        val description: String? = null,
        val contacts: MutableSet<String>? = null,
        val flags: MutableSet<String>? = null,
        val metadata: MutableMap<String, Any>? = null
) {

    companion object {
        fun toMetadataUpdate(update: SecurableObjectMetadataUpdate): MetadataUpdate {
            return MetadataUpdate(update.title, update.description, update.contacts)
        }

        fun fromMetadataUpdate(update: MetadataUpdate): SecurableObjectMetadataUpdate {
            val title = if (update.title.isPresent) update.title.get() else null
            val description = if (update.description.isPresent) update.description.get() else null
            val contacts = if (update.contacts.isPresent) update.contacts.get() else null

            return SecurableObjectMetadataUpdate(title, description, contacts)
        }
    }
}