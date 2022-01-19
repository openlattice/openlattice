package com.openlattice.collaborations

import com.geekbeast.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import java.util.*

data class AlterOrganizationsInCollaborationEntryProcessor(
        val organizationIds: Set<UUID>,
        val isAdding: Boolean
) : AbstractRhizomeEntryProcessor<UUID, Collaboration, Boolean>() {

    override fun process(entry: MutableMap.MutableEntry<UUID, Collaboration>): Boolean {
        val collaboration = entry.value

        if (isAdding) {
            collaboration.addOrganizationIds(organizationIds)
        } else {
            collaboration.removeOrganizationIds(organizationIds)
        }
        entry.setValue(collaboration)

        return true
    }
}