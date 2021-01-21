package com.openlattice.postgres.pods

import com.openlattice.authorization.DbCredentialService
import com.openlattice.authorization.PrincipalsMapManager
import com.openlattice.postgres.external.ExternalDatabaseConnectionManager
import com.openlattice.postgres.external.ExternalDatabasePermissioner
import com.openlattice.postgres.external.ExternalDatabasePermissioningService
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import javax.inject.Inject

/**
 * @author Drew Bailey (drew@openlattice.com)
 */
@Configuration
class ExternalDatabasePermissionsManagerPod {

    @Inject
    private lateinit var extDbManager: ExternalDatabaseConnectionManager

    @Inject
    private lateinit var dbCredentialService: DbCredentialService

    @Inject
    private lateinit var principalsMapManager: PrincipalsMapManager

    @Bean
    fun externalDatabasePermissionsManager(): ExternalDatabasePermissioningService {
        LoggerFactory.getLogger(ExternalDatabaseConnectionManagerPod::class.java).info("Constructing ExternalDatabaseConnectionManager")
        return ExternalDatabasePermissioner(
                extDbManager,
                dbCredentialService,
                principalsMapManager
        )
    }
}
