package com.openlattice.apps.processors

import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.apps.App
import com.openlattice.apps.AppRole
import java.util.*

class AddRoleToAppProcessor(val role: AppRole) : AbstractRhizomeEntryProcessor<UUID, App, App>() {

    override fun process(entry: MutableMap.MutableEntry<UUID, App>?): App? {
        val app = entry?.value ?: return null
        app.addRole(role)
        return app
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as AddRoleToAppProcessor

        if (role != other.role) return false

        return true
    }

    override fun hashCode(): Int {
        return role.hashCode()
    }


}