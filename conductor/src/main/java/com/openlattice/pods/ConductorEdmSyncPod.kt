package com.openlattice.pods

import com.hazelcast.core.HazelcastInstance
import com.openlattice.datastore.services.EdmManager
import com.openlattice.edm.tasks.EdmSyncInitializerDependencies
import com.openlattice.edm.tasks.EdmSyncInitializerTask
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.core.env.Environment
import org.springframework.core.env.Profiles
import javax.inject.Inject

private const val EDM_SYNC_CONFIGURATION = "edmsync"

/**
 * Syncs the EDM from production if the [EDM_SYNC_CONFIGURATION] profile is active.
 */
@Configuration
class ConductorEdmSyncPod
@Inject constructor(
        val edmManager: EdmManager,
        val environment: Environment,
        val hazelcast: HazelcastInstance
) {

    @Bean
    fun edmSyncInitializerDependencies(): EdmSyncInitializerDependencies {
        return EdmSyncInitializerDependencies(edmManager, environment.acceptsProfiles(Profiles.of(EDM_SYNC_CONFIGURATION)), hazelcast)
    }

    @Bean
    fun edmSyncInitializerTask(): EdmSyncInitializerTask {
        return EdmSyncInitializerTask()
    }
}