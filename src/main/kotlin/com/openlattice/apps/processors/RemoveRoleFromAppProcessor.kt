package com.openlattice.apps.processors

import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.apps.App
import java.util.*

data class RemoveRoleFromAppProcessor(val roleId: UUID) : AbstractRhizomeEntryProcessor<UUID, App, App>() {

    override fun process(entry: MutableMap.MutableEntry<UUID, App>): App? {
        val app = entry.value
        app.removeRole(roleId)
        entry.setValue(app)
        return app
    }

}