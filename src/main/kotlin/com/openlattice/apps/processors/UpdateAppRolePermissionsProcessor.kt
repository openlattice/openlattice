package com.openlattice.apps.processors

import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.apps.App
import com.openlattice.authorization.Permission
import java.util.*
import kotlin.collections.MutableMap.MutableEntry

data class UpdateAppRolePermissionsProcessor(
        val roleId: UUID,
        val permissions: Map<Permission, Map<UUID, Optional<Set<UUID>>>>
) : AbstractRhizomeEntryProcessor<UUID, App?, Unit>() {

    override fun process(entry: MutableEntry<UUID, App?>) {
        val app = entry.value
        if (app != null) {
            app.setRolePermissions(roleId, permissions)
            entry.setValue(app)
        }
    }
}