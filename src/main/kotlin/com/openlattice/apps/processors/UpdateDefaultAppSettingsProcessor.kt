package com.openlattice.apps.processors

import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.apps.App
import java.util.*

data class UpdateDefaultAppSettingsProcessor(
        val newSettings: MutableMap<String, Any>
) : AbstractRhizomeEntryProcessor<UUID, App, App>() {

    override fun process(entry: MutableMap.MutableEntry<UUID, App>): App? {
        val app = entry.value
        app.defaultSettings = newSettings
        entry.setValue(app)

        return app
    }
}