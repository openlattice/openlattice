package com.openlattice.codex

import com.openlattice.authorization.HazelcastAclKeyReservationService
import com.openlattice.collections.CollectionsManager
import com.openlattice.tasks.HazelcastTaskDependencies


class CodexInitializationTaskDependencies(
        val reservations: HazelcastAclKeyReservationService,
        val collectionManager: CollectionsManager
) : HazelcastTaskDependencies