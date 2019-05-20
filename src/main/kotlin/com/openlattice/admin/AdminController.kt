package com.openlattice.admin

import com.codahale.metrics.annotation.Timed
import com.google.common.collect.Iterables
import com.hazelcast.core.HazelcastInstance
import com.openlattice.auditing.AuditRecordEntitySetsManager
import com.openlattice.authorization.AuthorizationManager
import com.openlattice.authorization.AuthorizingComponent
import com.openlattice.authorization.Principal
import com.openlattice.authorization.Principals
import com.openlattice.data.storage.MetadataOption
import com.openlattice.data.storage.selectEntitySetWithCurrentVersionOfPropertyTypes
import com.openlattice.datastore.services.EdmManager
import com.openlattice.edm.PostgresEdmManager
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.postgres.DataTables.quote
import org.apache.olingo.commons.api.edm.EdmPrimitiveType
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind
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

    @Inject
    private lateinit var edm: EdmManager

    @GetMapping(value = [SQL + ID_PATH], produces = [MediaType.APPLICATION_JSON_VALUE])
    override fun getEntitySetSql(
            @PathVariable(ID) entitySetId: UUID,
            @RequestParam(OMIT_ENTITY_SET_ID, defaultValue = "false") omitEntitySetId: Boolean
    ): String {
        ensureAdminAccess()
        val entitySet = edm.getEntitySet(entitySetId)
        return if (entitySet.isLinking) {
            buildEntitySetSql(
                    entitySet.linkedEntitySets.associateWith { Optional.empty<Set<UUID>>() }, true, omitEntitySetId
            )
        } else {
            buildEntitySetSql(mapOf(entitySetId to Optional.empty()), false, omitEntitySetId)
        }
    }

    @PostMapping(
            value = [SQL],
            consumes = [MediaType.APPLICATION_JSON_VALUE],
            produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun getEntitySetSql(
            @RequestBody entityKeyIds: Map<UUID, Optional<Set<UUID>>>,
            @RequestParam(LINKING, defaultValue = "false") linking: Boolean,
            @RequestParam(OMIT_ENTITY_SET_ID, defaultValue = "false") omitEntitySetId: Boolean
    ): String {
        ensureAdminAccess()
        return buildEntitySetSql(entityKeyIds, linking, omitEntitySetId)
    }

    private fun buildEntitySetSql(
            entityKeyIds: Map<UUID, Optional<Set<UUID>>>, linking: Boolean, omitEntitySetId: Boolean
    ): String {
        val entityTypeId = Iterables.getOnlyElement(
                edm.getEntitySetsAsMap(entityKeyIds.keys).values.map { it.entityTypeId }.toSet()
        )
        val entityType = edm.getEntityType(entityTypeId)
        val propertyTypes = edm.getPropertyTypesAsMap(entityType.properties)
        //TODO: Get rid of query id.
        return selectEntitySetWithCurrentVersionOfPropertyTypes(
                UUID(0,0),
                entityKeyIds,
                propertyTypes.mapValues { quote(it.value.type.fullQualifiedNameAsString) },
                entityType.properties,
                entityKeyIds.keys.associateWith { entityType.properties },
                mapOf(),
                EnumSet.allOf(MetadataOption::class.java),
                propertyTypes.mapValues { it.value.datatype == EdmPrimitiveTypeKind.Binary },
                linking,
                omitEntitySetId
        )

    }


    @Timed
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

    @Timed
    @GetMapping(value = [RELOAD_CACHE + NAME_PATH])
    override fun reloadCache(@PathVariable(NAME) name: String) {
        ensureAdminAccess()
        hazelcast.getMap<Any, Any>(HazelcastMap.valueOf(name).name).loadAll(true)
    }

    @Timed
    @GetMapping(value = [PRINCIPALS + ID_PATH])
    override fun getUserPrincipals(@PathVariable(ID) principalId: String): Set<Principal> {
        ensureAdminAccess()
        return Principals.getUserPrincipals(principalId)
    }

    @Timed
    @PostMapping(
            value = [ENTITY_SETS + COUNT], consumes = [MediaType.APPLICATION_JSON_VALUE],
            produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun countEntitySetsOfEntityTypes(@RequestBody entityTypeIds: Set<UUID>): Map<UUID, Long> {
        ensureAdminAccess()
        return postgresEdmManager.countEntitySetsOfEntityTypes(entityTypeIds)
    }

    override fun getAuthorizationManager(): AuthorizationManager {
        return authorizationManager
    }

}