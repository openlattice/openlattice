package com.openlattice.collaborations

import com.hazelcast.core.HazelcastInstance
import com.openlattice.authorization.HazelcastAclKeyReservationService
import com.openlattice.authorization.Principal
import com.openlattice.hazelcast.HazelcastMap
import java.util.*

class CollaborationService(
        hazelcast: HazelcastInstance,
        private val aclKeyReservationService: HazelcastAclKeyReservationService,
        private val authorizationService:
) {

    private val collaborations = HazelcastMap.COLLABORATIONS.getMap(hazelcast)

    fun getCollaborations(ids: Set<UUID>): Map<UUID, Collaboration> {
        return collaborations.getAll(ids)

    }

    fun getCollaboration(id: UUID): Collaboration {
        return collaborations.getValue(id)
    }

    fun createCollaboration(collaboration: Collaboration, ownerPrincipal: Principal): UUID {

    }

    fun deleteCollaboration(id: UUID) {

    }

    fun addOrganizationIdsToCollaboration(id: UUID, organizationIds: Set<UUID>) {

    }

    fun removeOrganizationIdsFromCollaboration(id: UUID, organizationIds: Set<UUID>) {

    }


}