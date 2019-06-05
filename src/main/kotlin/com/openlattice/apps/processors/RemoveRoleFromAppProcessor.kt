package com.openlattice.apps.processors

import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.apps.App
import java.util.*

class RemoveRoleFromAppProcessor(val roleId: UUID) : AbstractRhizomeEntryProcessor<UUID, App, App>() {

    override fun process(entry: MutableMap.MutableEntry<UUID, App>?): App? {
        val app = entry?.value ?: return null
        app.removeRole(roleId)
        return app
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as RemoveRoleFromAppProcessor

        if (roleId != other.roleId) return false

        return true
    }

    override fun hashCode(): Int {
        return roleId.hashCode()
    }


}