package com.openlattice.hazelcast.processors

import com.hazelcast.core.Offloadable
import com.hazelcast.spi.impl.executionservice.ExecutionService
import com.openlattice.authorization.Principal
import com.openlattice.organizations.Organization
import com.openlattice.rhizome.hazelcast.entryprocessors.AbstractReadOnlyRhizomeEntryProcessor
import java.util.*

/**
 * @author Drew Bailey &lt;drew@openlattice.com&gt;
 */
class GetMembersOfOrganizationEntryProcessor:
        AbstractReadOnlyRhizomeEntryProcessor<UUID, Organization, Set<Principal>>(),
        Offloadable {

    override fun process(entry: MutableMap.MutableEntry<UUID, Organization?>): Set<Principal>? {
        val maybeValue = entry.value ?: return null
        return maybeValue.members
    }

    override fun getExecutorName(): String {
        return ExecutionService.OFFLOADABLE_EXECUTOR
    }
}