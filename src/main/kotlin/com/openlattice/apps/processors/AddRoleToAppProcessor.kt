package com.openlattice.apps.processors

import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.apps.App
import com.openlattice.apps.AppRole
import java.util.*

data class AddRoleToAppProcessor(val role: AppRole) : AbstractRhizomeEntryProcessor<UUID, App, App>() {

    override fun process(entry: MutableMap.MutableEntry<UUID, App>): App? {
        val app = entry.value
        app.addRole(role)
        entry.setValue(app)
        return app
    }

}