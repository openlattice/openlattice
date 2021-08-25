package com.openlattice.admin

import com.codahale.metrics.annotation.Timed
import com.geekbeast.rhizome.jobs.AbstractDistributedJob
import com.geekbeast.rhizome.jobs.DistributableJob
import com.geekbeast.rhizome.jobs.HazelcastJobService
import com.geekbeast.rhizome.jobs.JobStatus
import com.google.common.collect.Iterables
import com.hazelcast.core.HazelcastInstance
import com.openlattice.authorization.AuthorizationManager
import com.openlattice.authorization.AuthorizingComponent
import com.openlattice.authorization.Principal
import com.openlattice.authorization.Principals
import com.openlattice.data.DataGraphManager
import com.openlattice.data.storage.MetadataOption
import com.openlattice.data.storage.PostgresEntityDataQueryService
import com.openlattice.data.storage.selectEntitySetWithCurrentVersionOfPropertyTypes
import com.openlattice.datastore.services.EdmManager
import com.openlattice.datastore.services.EntitySetManager
import com.openlattice.edm.PostgresEdmManager
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.jobs.JobUpdate
import com.openlattice.notifications.sms.SmsEntitySetInformation
import com.openlattice.organizations.HazelcastOrganizationService
import com.openlattice.organizations.Organization
import com.openlattice.postgres.DataTables.quote
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind
import org.slf4j.LoggerFactory
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.*
import java.util.*
import javax.inject.Inject

@SuppressFBWarnings(
        value = ["RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE", "BC_BAD_CAST_TO_ABSTRACT_COLLECTION"],
        justification = "Allowing redundant kotlin null check on lateinit variables, " +
                "Allowing kotlin collection mapping cast to List"
)
@RestController
@RequestMapping(CONTROLLER)
class AdminController : AdminApi, AuthorizingComponent {
    companion object {

        private val logger = LoggerFactory.getLogger(AdminController::class.java)!!

    }

    @Inject
    private lateinit var authorizationManager: AuthorizationManager

    @Inject
    private lateinit var hazelcast: HazelcastInstance

    @Inject
    private lateinit var postgresEdmManager: PostgresEdmManager

    @Inject
    private lateinit var edm: EdmManager

    @Inject
    private lateinit var entitySetManager: EntitySetManager

    @Inject
    private lateinit var organizations: HazelcastOrganizationService

    @Inject
    private lateinit var pedqs: PostgresEntityDataQueryService

    @Inject
    private lateinit var dgm: DataGraphManager

    @Inject
    private lateinit var jobService: HazelcastJobService

    @GetMapping(value = [SQL + ID_PATH], produces = [MediaType.APPLICATION_JSON_VALUE])
    override fun getEntitySetSql(
            @PathVariable(ID) entitySetId: UUID,
            @RequestParam(OMIT_ENTITY_SET_ID, defaultValue = "false") omitEntitySetId: Boolean
    ): String {
        ensureAdminAccess()
        val entitySet = entitySetManager.getEntitySet(entitySetId)!!
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
                entitySetManager.getEntitySetsAsMap(entityKeyIds.keys).values.map { it.entityTypeId }.toSet()
        )
        val entityType = edm.getEntityType(entityTypeId)
        val propertyTypes = edm.getPropertyTypesAsMap(entityType.properties)

        return selectEntitySetWithCurrentVersionOfPropertyTypes(
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
    @SuppressFBWarnings("NP_ALWAYS_NULL", justification="Issue with spotbugs handling of Kotlin")
    override fun reloadCache() {
        ensureAdminAccess()
        HazelcastMap.values().forEach {
            logger.info("Reloading map $it")
            try {
                it.getMap(hazelcast).loadAll(true)
            } catch (e: IllegalArgumentException) {
                logger.error("Unable to reload map $it", e)
            }
        }
    }

    @Timed
    @GetMapping(value = [RELOAD_CACHE + NAME_PATH])
    override fun reloadCache(@PathVariable(NAME) name: String) {
        ensureAdminAccess()
        HazelcastMap.valueOf(name).getMap(hazelcast).loadAll(true)
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

    @Timed
    @PostMapping(value = [ID_PATH + PHONE], consumes = [MediaType.APPLICATION_JSON_VALUE])
    override// Hopefully spring is in the frameworks that accepts plain quoted string as a valid value.
    fun setOrganizationEntitySetInformation(
            @PathVariable(ID) organizationId: UUID,
            @RequestBody entitySetInformationList: List<SmsEntitySetInformation>
    ): Int? {
        ensureAdminAccess()
        organizations.setSmsEntitySetInformation(entitySetInformationList)
        return entitySetInformationList.size
    }

    @Timed
    @GetMapping(value = [ORGANIZATION + USAGE], produces = [MediaType.APPLICATION_JSON_VALUE])
    override fun getEntityCountByOrganization(): Map<UUID, Long> {
        ensureAdminAccess()

        val unassignedId = UUID(0, 0)

        val entitySetCounts = pedqs.getEntitySetCounts()
        val entitySets = entitySetManager.getEntitySetsAsMap(entitySetCounts.keys)

        val orgCounts = mutableMapOf<UUID, Long>()
        entitySetCounts.forEach { (entitySetId, count) ->
            val orgId = entitySets[entitySetId]?.organizationId ?: unassignedId
            orgCounts[orgId] = orgCounts.getOrDefault(orgId, 0) + count
        }

        return orgCounts
    }

    @Timed
    @GetMapping(value = [ORGANIZATION], produces = [MediaType.APPLICATION_JSON_VALUE])
    override fun getAllOrganizations(): Iterable<Organization> {
        ensureAdminAccess()
        return organizations.getAllOrganizations()
    }

    @Timed
    @GetMapping(value = [JOBS], produces = [MediaType.APPLICATION_JSON_VALUE])
    override fun getJobs(): Map<UUID, DistributableJob<*>> {
        ensureAdminAccess()
        return jobService.getJobs()
    }

    @Timed
    @PostMapping(value = [JOBS], produces = [MediaType.APPLICATION_JSON_VALUE])
    override fun getJobs(@RequestBody statuses: Set<JobStatus>): Map<UUID, DistributableJob<*>> {
        ensureAdminAccess()
        return jobService.getJobs(statuses)
    }

    @Timed
    @GetMapping(value = [JOBS + ID_PATH], produces = [MediaType.APPLICATION_JSON_VALUE])
    override fun getJob(@PathVariable(ID) jobId: UUID): Map<UUID, DistributableJob<*>> {
        ensureAdminAccess()
        return jobService.getJobs(listOf(jobId))
    }

    @Timed
    @PatchMapping(value = [JOBS], produces = [MediaType.APPLICATION_JSON_VALUE])
    override fun createJobs(jobs: List<DistributableJob<*>>): List<UUID> {
        ensureAdminAccess()
        return jobs.map { jobService.submitJob(it as AbstractDistributedJob<*, *>) }
    }

    @Timed
    @PatchMapping(
            value = [JOBS + ID_PATH],
            produces = [MediaType.APPLICATION_JSON_VALUE],
            consumes = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun updateJob(
            @PathVariable(ID) jobId: UUID,
            @RequestBody update: JobUpdate
    ): Map<UUID, AbstractDistributedJob<*, *>> {
        ensureAdminAccess()
        val jobs = setOf(jobId)
        jobService.updateJob(jobId, update.status)

        if (update.reload) jobService.reload(jobs, true)

        return jobService.getJobs(jobs)

    }

    override fun getAuthorizationManager(): AuthorizationManager {
        return authorizationManager
    }

}