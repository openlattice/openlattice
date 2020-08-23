package com.openlattice.apps.processors

import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.apps.App
import com.openlattice.edm.requests.MetadataUpdate
import java.util.*
import kotlin.collections.MutableMap.MutableEntry

data class UpdateAppMetadataProcessor(val update: MetadataUpdate) : AbstractRhizomeEntryProcessor<UUID, App?, Unit>() {

    override fun process(entry: MutableEntry<UUID, App?>) {
        val app = entry.value
        if (app != null) {
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
        }
    }

}