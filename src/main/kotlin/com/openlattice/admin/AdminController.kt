package com.openlattice.admin

import com.hazelcast.core.HazelcastInstance
import com.openlattice.auditing.AuditRecordEntitySetsManager
import com.openlattice.authorization.AuthorizationManager
import com.openlattice.authorization.AuthorizingComponent
import com.openlattice.authorization.Principal
import com.openlattice.authorization.Principals
import com.openlattice.edm.PostgresEdmManager
import com.openlattice.hazelcast.HazelcastMap
import org.slf4j.LoggerFactory
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.*
import java.util.*
import javax.inject.Inject

@RestController
@RequestMapping(CONTROLLER)
class AdminController : AdminApi, AuthorizingComponent {

    companion object {
        private val logger = LoggerFactory.getLogger(AdminController::class.java)!!
    }

    @Inject
    private lateinit var auditRecordEntitySetsManager: AuditRecordEntitySetsManager

    @Inject
    private lateinit var authorizationManager: AuthorizationManager

    @Inject
    private lateinit var hazelcast: HazelcastInstance

    @Inject
    private lateinit var postgresEdmManager: PostgresEdmManager


    @GetMapping(value = [RELOAD_CACHE])
    override fun reloadCache() {
        ensureAdminAccess()
        HazelcastMap.values().forEach {
            logger.info("Reloading map ${it.name}")
            try {
                hazelcast.getMap<Any, Any>(it.name).loadAll(true)
            } catch (e: IllegalArgumentException) {
                logger.error("Unable to reload map ${it.name}", e)
            }
        }
    }

    @GetMapping(value = [RELOAD_CACHE + NAME_PATH])
    override fun reloadCache(@PathVariable(NAME) name: String) {
        ensureAdminAccess()
        hazelcast.getMap<Any, Any>(HazelcastMap.valueOf(name).name).loadAll(true)
    }

    @GetMapping(value = [PRINCIPALS + ID_PATH])
    override fun getUserPrincipals(@PathVariable(ID) principalId: String): Set<Principal> {
        ensureAdminAccess()
        return Principals.getUserPrincipals(principalId)
    }

    @PostMapping(value = [ENTITY_SETS + COUNT], consumes = [MediaType.APPLICATION_JSON_VALUE],
            produces = [MediaType.APPLICATION_JSON_VALUE])
    override fun countEntitySetsOfEntityTypes(@RequestBody entityTypeIds: Set<UUID>): Map<UUID, Long> {
        ensureAdminAccess()
        return postgresEdmManager.countEntitySetsOfEntityTypes(entityTypeIds)
    }

    override fun getAuthorizationManager(): AuthorizationManager {
        return authorizationManager
    }

}