package com.openlattice.apps.processors

import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.apps.AppConfigKey
import com.openlattice.apps.AppTypeSetting

data class UpdateAppConfigSettingsProcessor(
        val settingsToAdd: Map<String, Any>,
        val settingsToRemove: Set<String>
) : AbstractRhizomeEntryProcessor<AppConfigKey, AppTypeSetting, AppTypeSetting>() {

    override fun process(entry: MutableMap.MutableEntry<AppConfigKey, AppTypeSetting>): AppTypeSetting? {
        val config = entry.value
        config.updateSettings(settingsToAdd)
        config.removeSettings(settingsToRemove)
        entry.setValue(config)
        return config
    }
}