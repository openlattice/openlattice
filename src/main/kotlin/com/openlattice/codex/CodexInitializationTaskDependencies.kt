package com.openlattice.codex

import com.hazelcast.core.HazelcastInstance
import com.openlattice.authorization.HazelcastAclKeyReservationService
import com.openlattice.tasks.HazelcastTaskDependencies


class CodexInitializationTaskDependencies(
        val reservations: HazelcastAclKeyReservationService,
        val hazelcast: HazelcastInstance
) : HazelcastTaskDependencies