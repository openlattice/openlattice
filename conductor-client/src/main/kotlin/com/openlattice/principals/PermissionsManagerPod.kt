package com.openlattice.principals

import com.google.common.eventbus.EventBus
import com.hazelcast.core.HazelcastInstance
import com.openlattice.authorization.AuthorizationManager
import com.openlattice.authorization.PrincipalsMapManager
import com.openlattice.authorization.HazelcastAclKeyReservationService
import com.openlattice.authorization.HazelcastAuthorizationService
import com.openlattice.organizations.roles.HazelcastPrincipalService
import com.openlattice.organizations.roles.SecurePrincipalsManager
import com.openlattice.postgres.external.ExternalDatabasePermissioningService
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import javax.inject.Inject

/**
 * @author Drew Bailey (drew@openlattice.com)
 */
@Configuration
class PermissionsManagerPod {

    @Inject
    private lateinit var hazelcastInstance: HazelcastInstance

    @Inject
    private lateinit var extDatabasePermsManager: ExternalDatabasePermissioningService

    @Inject
    private lateinit var eventBus: EventBus

    @Inject
    private lateinit var aclKeyReservationService: HazelcastAclKeyReservationService

    @Inject
    private lateinit var principalsMapManager: PrincipalsMapManager

    @Bean
    fun authorizationManager(): AuthorizationManager {
        return HazelcastAuthorizationService(hazelcastInstance, eventBus)
    }

    @Bean
    fun principalService(): SecurePrincipalsManager {
        LoggerFactory.getLogger(PermissionsManagerPod::class.java).info("Constructing SecurePrincipalsManager")
        return HazelcastPrincipalService(
                hazelcastInstance,
                aclKeyReservationService,
                authorizationManager(),
                principalsMapManager,
                extDatabasePermsManager
        )
    }
}