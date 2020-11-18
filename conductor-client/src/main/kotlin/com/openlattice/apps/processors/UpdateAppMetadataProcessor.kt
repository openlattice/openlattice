package com.openlattice.apps.processors

import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.apps.App
import com.openlattice.edm.requests.MetadataUpdate
import java.util.*
import kotlin.collections.MutableMap.MutableEntry

data class UpdateAppMetadataProcessor(val update: MetadataUpdate) : AbstractRhizomeEntryProcessor<UUID, App?, Boolean>() {

    override fun process(entry: MutableEntry<UUID, App?>): Boolean {
        val app = entry.value ?: return false

        if (update.title.isPresent) {
            app.title = update.title.get()
        }
        if (update.description.isPresent) {
            app.description = update.description.get()
        }
        if (update.name.isPresent) {
            app.name = update.name.get()
        }
        if (update.url.isPresent) {
            app.url = update.url.get()
        }
        entry.setValue(app)

        return true
    }

}