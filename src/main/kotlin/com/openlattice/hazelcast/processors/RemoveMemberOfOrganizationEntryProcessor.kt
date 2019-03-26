package com.openlattice.hazelcast.processors

import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.authorization.Principal
import com.openlattice.authorization.PrincipalType
import com.openlattice.organizations.PrincipalSet
import java.util.*

data class RemoveMemberOfOrganizationEntryProcessor(val memberIds: Set<String> ) :
        AbstractRhizomeEntryProcessor<UUID, PrincipalSet, Void?>() {

    override fun process( entry: MutableMap.MutableEntry<UUID, PrincipalSet>? ): Void? {
        val membersToRemove = memberIds.map { userId -> Principal( PrincipalType.USER, userId ) }
        val currentMembers = entry!!.value

        currentMembers.removeAll( membersToRemove )

        entry.setValue( currentMembers )
        return null;
    }


}
