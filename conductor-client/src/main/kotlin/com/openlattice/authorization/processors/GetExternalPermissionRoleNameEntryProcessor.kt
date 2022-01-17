package com.openlattice.authorization.processors

import com.geekbeast.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.authorization.AccessTarget
import java.util.*

/**
 * @author Drew Bailey (drew@openlattice.com)
 */
class GetExternalPermissionRoleNameEntryProcessor: AbstractRhizomeEntryProcessor<AccessTarget, UUID, String>() {
    override fun process(entry: MutableMap.MutableEntry<AccessTarget, UUID?>): String {
        val value = entry.value
        if ( value != null ){
            return value.toString()
        }

        val newName = UUID.randomUUID()
        entry.setValue(newName)
        return newName.toString()
    }
}