package com.openlattice.codex

import com.hazelcast.core.HazelcastInstance
import com.openlattice.tasks.HazelcastTaskDependencies

class CodexMessageSyncTaskDependencies(
        val hazelcast: HazelcastInstance,
        val codexService: CodexService
) : HazelcastTaskDependencies