package com.openlattice.hazelcast.processors

import com.kryptnostic.rhizome.hazelcast.processors.AbstractRemover
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.authorization.Principal
import com.openlattice.authorization.PrincipalType
import com.openlattice.organizations.PrincipalSet
import java.util.*

data class RemoveMemberOfOrganizationEntryProcessor(
        val memberIds: Set<String>
) : AbstractRemover<UUID, PrincipalSet, Principal>(memberIds.map { Principal(PrincipalType.USER, it) })
