package com.openlattice.admin

import com.hazelcast.core.HazelcastInstance
import com.openlattice.auditing.AuditRecordEntitySetsManager
import com.openlattice.authorization.AuthorizationManager
import com.openlattice.authorization.AuthorizingComponent
import com.openlattice.authorization.Principal
import com.openlattice.authorization.Principals
import com.openlattice.datastore.services.EdmManager
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.organizations.PrincipalSet
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.*
import java.util.*
import javax.inject.Inject

@RestController
@RequestMapping(CONTROLLER)
class AdminController : AdminApi, AuthorizingComponent {

    @Inject
    private lateinit var auditRecordEntitySetsManager: AuditRecordEntitySetsManager

    @Inject
    private lateinit var authorizationManager: AuthorizationManager

    @Inject
    private lateinit var hazelcast: HazelcastInstance

    @Inject
    private lateinit var edmManager: EdmManager


    @GetMapping(value = [RELOAD_CACHE])
    override fun reloadCache() {
        ensureAdminAccess()
        HazelcastMap.values().forEach { hazelcast.getMap<Any, Any>(it.name).loadAll(true) }
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
    override fun countEntitySetsOfEntityTypes(@RequestBody entityTypeIds: Set<UUID>): Map<UUID, Int> {
        ensureAdminAccess()
        return edmManager.countEntitySetsOfEntityTypes(entityTypeIds)
    }

    override fun getAuthorizationManager(): AuthorizationManager {
        return authorizationManager
    }

}