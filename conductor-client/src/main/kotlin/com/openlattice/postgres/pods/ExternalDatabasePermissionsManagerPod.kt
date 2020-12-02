package com.openlattice.postgres.pods

import com.google.common.eventbus.EventBus
import com.openlattice.authorization.AuthorizationManager
import com.openlattice.authorization.DbCredentialService
import com.openlattice.organizations.roles.SecurePrincipalsManager
import com.openlattice.postgres.external.ExternalDatabaseConnectionManager
import com.openlattice.postgres.external.ExternalDatabasePermissionsManager
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import javax.inject.Inject

/**
 * @author Drew Bailey (drewbaileym@gmail.com)
 */
@Configuration
class ExternalDatabasePermissionsManagerPod {

    @Inject
    private lateinit var extDbManager: ExternalDatabaseConnectionManager

    @Inject
    private lateinit var dbCredentialService: DbCredentialService

    @Inject
    private lateinit var securePrincipalsManager: SecurePrincipalsManager

    @Inject
    private lateinit var permissionsManager: AuthorizationManager

    @Inject
    private lateinit var eventBus: EventBus

    @Bean
    fun externalDatabasePermissionsManager(): ExternalDatabasePermissionsManager{
        LoggerFactory.getLogger(ExternalDatabaseConnectionManagerPod::class.java).info("Constructing ExternalDatabaseConnectionManager")
        return ExternalDatabasePermissionsManager(extDbManager, dbCredentialService, securePrincipalsManager, permissionsManager, eventBus)
    }
}
