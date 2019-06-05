package com.openlattice.apps.processors

import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.apps.AppConfigKey
import com.openlattice.apps.AppTypeSetting
import java.util.*

class RemoveRoleFromAppConfigProcessor(val roleId: UUID) : AbstractRhizomeEntryProcessor<AppConfigKey, AppTypeSetting, AppTypeSetting>() {

    override fun process(entry: MutableMap.MutableEntry<AppConfigKey, AppTypeSetting>?): AppTypeSetting? {
        val setting = entry?.value ?: return null
        setting.removeRole(roleId)
        return setting
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as RemoveRoleFromAppConfigProcessor

        if (roleId != other.roleId) return false

        return true
    }

    override fun hashCode(): Int {
        return roleId.hashCode()
    }

}