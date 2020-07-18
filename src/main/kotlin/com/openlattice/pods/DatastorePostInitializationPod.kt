package com.openlattice.pods

import com.hazelcast.core.HazelcastInstance
import com.openlattice.codex.CodexMessageSyncTask
import com.openlattice.codex.CodexMessageSyncTaskDependencies
import com.openlattice.codex.CodexService
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import javax.inject.Inject

@Configuration
class DatastorePostInitializationPod {

    @Inject
    private lateinit var hazelcast: HazelcastInstance

    @Inject
    private lateinit var codexService: CodexService

    @Bean
    fun codexMessageSyncTaskDependencies(): CodexMessageSyncTaskDependencies {
        return CodexMessageSyncTaskDependencies(hazelcast, codexService)
    }

    @Bean
    fun codexMessageSyncTask(): CodexMessageSyncTask {
        return CodexMessageSyncTask()
    }
}