package com.openlattice.apps.processors

import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.apps.AppConfigKey
import com.openlattice.apps.AppTypeSetting

class UpdateAppConfigSettingsProcessor(
        val settingsToAdd: Map<String, Any>,
        val settingsToRemove: Set<String>
) : AbstractRhizomeEntryProcessor<AppConfigKey, AppTypeSetting, AppTypeSetting>() {

    override fun process(entry: MutableMap.MutableEntry<AppConfigKey, AppTypeSetting>?): AppTypeSetting? {
        val config = entry?.value ?: return null
        config.updateSettings(settingsToAdd)
        config.removeSettings(settingsToRemove)
        return config
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as UpdateAppConfigSettingsProcessor

        if (settingsToAdd != other.settingsToAdd) return false

        return true
    }

    override fun hashCode(): Int {
        return settingsToAdd.hashCode()
    }


}