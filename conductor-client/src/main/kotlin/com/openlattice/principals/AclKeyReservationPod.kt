package com.openlattice.principals

import com.hazelcast.core.HazelcastInstance
import com.openlattice.authorization.HazelcastPrincipalsMapManager
import com.openlattice.authorization.PrincipalsMapManager
import com.openlattice.authorization.HazelcastAclKeyReservationService
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import javax.inject.Inject

/**
 * @author Drew Bailey (drew@openlattice.com)
 */
@Configuration
class AclKeyReservationPod {

    @Inject
    private lateinit var hazelcastInstance: HazelcastInstance

    @Bean
    fun aclKeyReservationService(): HazelcastAclKeyReservationService {
        return HazelcastAclKeyReservationService(hazelcastInstance)
    }

    @Bean
    fun principalsManager(): PrincipalsMapManager {
        return HazelcastPrincipalsMapManager(hazelcastInstance, aclKeyReservationService())
    }
}