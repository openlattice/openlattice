package com.openlattice.apps.processors

import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.apps.AppConfigKey
import com.openlattice.apps.AppTypeSetting
import com.openlattice.authorization.AclKey
import java.util.*

data class AddRoleToAppConfigProcessor(
        val roleId: UUID,
        val roleAclKey: AclKey
) : AbstractRhizomeEntryProcessor<AppConfigKey, AppTypeSetting, AppTypeSetting>() {

    override fun process(entry: MutableMap.MutableEntry<AppConfigKey, AppTypeSetting>): AppTypeSetting? {
        val setting = entry.value
        setting.addRole(roleId, roleAclKey)
        entry.setValue(setting)
        return setting
    }

}