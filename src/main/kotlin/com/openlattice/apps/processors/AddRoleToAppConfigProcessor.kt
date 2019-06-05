package com.openlattice.apps.processors

import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.apps.AppConfigKey
import com.openlattice.apps.AppTypeSetting
import com.openlattice.authorization.AclKey
import java.util.*

class AddRoleToAppConfigProcessor(val roleId: UUID, val roleAclKey: AclKey) : AbstractRhizomeEntryProcessor<AppConfigKey, AppTypeSetting, AppTypeSetting>() {

    override fun process(entry: MutableMap.MutableEntry<AppConfigKey, AppTypeSetting>?): AppTypeSetting? {
        val setting = entry?.value ?: return null
        setting.addRole(roleId, roleAclKey)
        return setting
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as AddRoleToAppConfigProcessor

        if (roleId != other.roleId) return false
        if (roleAclKey != other.roleAclKey) return false

        return true
    }

    override fun hashCode(): Int {
        var result = roleId.hashCode()
        result = 31 * result + roleAclKey.hashCode()
        return result
    }


}