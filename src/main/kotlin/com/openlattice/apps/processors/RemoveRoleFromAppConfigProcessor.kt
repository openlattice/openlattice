package com.openlattice.apps.processors

import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.apps.AppConfigKey
import com.openlattice.apps.AppTypeSetting
import java.util.*

data class RemoveRoleFromAppConfigProcessor(
        val roleId: UUID
) : AbstractRhizomeEntryProcessor<AppConfigKey, AppTypeSetting, AppTypeSetting>() {

    override fun process(entry: MutableMap.MutableEntry<AppConfigKey, AppTypeSetting>): AppTypeSetting? {
        val setting = entry.value
        setting.removeRole(roleId)
        entry.setValue(setting)
        return setting
    }

}