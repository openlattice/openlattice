package com.openlattice.apps.processors

import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.apps.App
import java.util.*

class UpdateDefaultAppSettingsProcessor(val newSettings: Map<String, Any>) : AbstractRhizomeEntryProcessor<UUID, App, App>() {

    override fun process(entry: MutableMap.MutableEntry<UUID, App>?): App? {
        val app = entry?.value ?: return null
        app.defaultSettings = newSettings
        entry.setValue(app)

        return app
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as UpdateDefaultAppSettingsProcessor

        if (newSettings != other.newSettings) return false

        return true
    }

    override fun hashCode(): Int {
        return newSettings.hashCode()
    }


}