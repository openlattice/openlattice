package com.openlattice.search

import com.google.common.collect.SetMultimap
import com.hazelcast.core.IMap
import com.openlattice.authorization.*
import com.openlattice.data.requests.NeighborEntityDetails
import com.openlattice.edm.EntitySet
import com.openlattice.edm.type.EntityType
import com.openlattice.edm.type.PropertyType
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.postgres.DataTables
import com.openlattice.postgres.DataTables.LAST_WRITE_FQN
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresTable.PERSISTENT_SEARCHES
import com.openlattice.postgres.ResultSetAdapters
import com.openlattice.postgres.streams.PostgresIterable
import com.openlattice.postgres.streams.StatementHolder
import com.openlattice.search.requests.DataSearchResult
import com.openlattice.search.requests.EntityNeighborsFilter
import com.openlattice.search.requests.PersistentSearch
import com.openlattice.search.requests.SearchConstraints
import com.openlattice.tasks.HazelcastFixedRateTask
import com.openlattice.tasks.HazelcastTaskDependencies
import com.openlattice.tasks.Task
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.LoggerFactory
import java.sql.ResultSet
import java.sql.Statement
import java.sql.Timestamp
import java.time.OffsetDateTime
import java.time.ZoneId
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.function.Function
import java.util.function.Supplier
import java.util.stream.Collectors

private val logger = LoggerFactory.getLogger(PersistentSearchMessengerTask::class.java)

const val ALERT_MESSENGER_INTERVAL_MILLIS = 60000L

private val LOAD_ACTIVE_ALERTS_SQL = "SELECT * FROM ${PERSISTENT_SEARCHES.name} WHERE ${EXPIRATION_DATE.name} > now()"

class PersistentSearchMessengerTask : HazelcastFixedRateTask<PersistentSearchMessengerTaskDependencies>, HazelcastTaskDependencies {

    override fun getInitialDelay(): Long {
        return ALERT_MESSENGER_INTERVAL_MILLIS
    }

    override fun getPeriod(): Long {
        return ALERT_MESSENGER_INTERVAL_MILLIS
    }

    override fun getTimeUnit(): TimeUnit {
        return TimeUnit.MILLISECONDS
    }

    override fun getName(): String {
        return Task.PERSISTENT_SEARCH_MESSENGER_TASK.name
    }

    override fun getDependenciesClass(): Class<out PersistentSearchMessengerTaskDependencies> {
        return PersistentSearchMessengerTaskDependencies::class.java
    }

    private fun getPropertyTypeIdsForEntitySet(entitySetId: UUID): Set<UUID> {
        val dependencies = getDependency()
        return dependencies.entityTypes[dependencies.entitySets[entitySetId]?.entityTypeId]?.properties.orEmpty()
    }

    private fun getAuthorizedPropertyMap(principals: Set<Principal>, entitySetId: UUID): Map<UUID, PropertyType> {
        val accessChecks = getPropertyTypeIdsForEntitySet(entitySetId).map { AccessCheck(AclKey(entitySetId, it), EnumSet.of(Permission.READ)) }.toMutableSet()
        return getDependency().propertyTypes.getAll(getDependency().authorizationManager.accessChecksForPrincipals(accessChecks, principals)
                .filter { it.permissions[Permission.READ]!! }
                .map { it.aclKey[1] }
                .collect(Collectors.toSet()))
    }

    private fun getAuthorizedPropertyTypeMap(securablePrincipal: SecurablePrincipal, entitySetIds: List<UUID>): Map<UUID, Map<UUID, PropertyType>> {
        var principals = getDependency().principalsManager.getAllPrincipals(securablePrincipal)
                .plus(securablePrincipal)
                .map { it.principal }
                .toSet()
        return entitySetIds.map { it to getAuthorizedPropertyMap(principals, it) }.toMap()
    }

    private fun getUpdatedConstraints(persistentSearch: PersistentSearch): SearchConstraints {
        val constraints: SearchConstraints = persistentSearch.searchConstraints
        val timeFilterConstraintGroup = SearchConstraints.writeDateTimeFilterConstraints(
                constraints.entitySetIds,
                constraints.start,
                constraints.maxHits,
                Optional.of(persistentSearch.lastRead),
                Optional.empty()).constraintGroups[0]
        constraints.constraintGroups.add(timeFilterConstraintGroup)
        return constraints
    }

    private fun sendAlertsForNewWrites(
            userSecurablePrincipal: SecurablePrincipal,
            persistentSearch: PersistentSearch,
            newResults: DataSearchResult,
            neighborsById: Map<UUID, List<NeighborEntityDetails>>) {
        val dependencies = getDependency()

        val userEmail = dependencies.principalsManager.getUser(userSecurablePrincipal.principal.id).email
        newResults.hits.forEach {
            val entityKeyId = UUID.fromString(it[DataTables.ID_FQN].first().toString())
            val renderableEmail = PersistentSearchEmailRenderer.renderEmail(persistentSearch, it, userEmail, neighborsById.getOrDefault(entityKeyId, listOf()), dependencies)
            dependencies.mailServiceClient.spool(renderableEmail)
        }
    }

    private fun getLatestRead(vehicleReads: List<SetMultimap<FullQualifiedName, Any>>): OffsetDateTime? {
        return vehicleReads
                .flatMap { it[LAST_WRITE_FQN] }
                .map {
                    val localDateTime = Timestamp.valueOf(it.toString()).toLocalDateTime()
                    return OffsetDateTime.of(localDateTime, ZoneId.systemDefault().rules.getOffset(localDateTime))
                }
                .sortedByDescending { it }
                .firstOrNull()
    }

    private fun getHitEntityKeyIds(hits: List<SetMultimap<FullQualifiedName, Any>>): Set<UUID> {
        return hits.map { UUID.fromString(it[DataTables.ID_FQN].first().toString()) }.toSet()
    }

    private fun findNewWritesForAlert(userAclKey: AclKey, persistentSearch: PersistentSearch): OffsetDateTime? {
        val dependencies = getDependency()

        val userSecurablePrincipal = dependencies.principalsManager.getSecurablePrincipal(userAclKey)
        val allUserPrincipals = dependencies.principalsManager.getAllPrincipals(userSecurablePrincipal).map { it.principal }.toSet()

        if (userSecurablePrincipal.principal == null || userSecurablePrincipal.principal.id == null) {
            logger.error("Failed to send persistent search for unrecognized principal {} with aclKey {}", userSecurablePrincipal, userAclKey)
            return null
        }

        val entitySets = dependencies.entitySets.getAll(persistentSearch.searchConstraints.entitySetIds.toSet()).values
                .groupBy { it.isLinking }

        val authorizedPropertyTypeMap = getAuthorizedPropertyTypeMap(userSecurablePrincipal,
                entitySets.getOrDefault(false, listOf()).map { it.id })
        val linkedAuthorizedPropertyTypeMap = getAuthorizedPropertyTypeMap(userSecurablePrincipal,
                entitySets.getOrDefault(true, listOf()).map { it.id })
        val constraints = getUpdatedConstraints(persistentSearch)

        val results = dependencies.searchService.executeSearch(constraints,
                authorizedPropertyTypeMap, false)
        val linkedResults = dependencies.searchService.executeSearch(constraints,
                linkedAuthorizedPropertyTypeMap, true)
        val newResults = DataSearchResult(results.numHits + linkedResults.numHits,
                results.hits + linkedResults.hits)

        if (newResults.numHits > 0) {
            val neighborsById = mutableMapOf<UUID, List<NeighborEntityDetails>>()

            if (results.hits.isNotEmpty()) neighborsById.putAll(dependencies.searchService.executeEntityNeighborSearch(
                    entitySets.getOrDefault(false, listOf()).map { it.id }.toSet(),
                    EntityNeighborsFilter(getHitEntityKeyIds(results.hits)),
                    allUserPrincipals)
            )

            if (linkedResults.hits.isNotEmpty()) neighborsById.putAll(dependencies.searchService.executeLinkingEntityNeighborSearch(
                    entitySets.getOrDefault(true, listOf()).map { it.id }.toSet(),
                    EntityNeighborsFilter(getHitEntityKeyIds(results.hits)),
                    allUserPrincipals)
            )

            sendAlertsForNewWrites(userSecurablePrincipal, persistentSearch, newResults, neighborsById)
            return getLatestRead(newResults.hits)
        }

        return null
    }

    private fun updateLastReadForAlert(alertId: UUID, readDateTime: OffsetDateTime) {
        val connection = getDependency().hds.connection
        connection.use {
            val stmt: Statement = connection.createStatement()
            stmt.execute(updateLastReadSql(readDateTime, alertId))
        }
    }

    override fun runTask() {
        logger.info("Loading new writes for persistent searches and sending alerts")

        val dependencies = getDependency()

        PostgresIterable(Supplier<StatementHolder> {
            val connection = dependencies.hds.connection
            val stmt = connection.createStatement()
            val rs = stmt.executeQuery(LOAD_ACTIVE_ALERTS_SQL)
            StatementHolder(connection, stmt, rs)
        }, Function<ResultSet, Pair<AclKey, PersistentSearch>> {
            ResultSetAdapters.aclKey(it) to ResultSetAdapters.persistentSearch(it)
        })
                .stream()
                .map { it.second.id to findNewWritesForAlert(it.first, it.second) }
                .filter { it.second != null }
                .forEach { updateLastReadForAlert(it.first, it.second!!) }

    }

    private fun updateLastReadSql(readDateTime: OffsetDateTime, id: UUID): String {
        return "UPDATE ${PERSISTENT_SEARCHES.name} SET ${LAST_READ.name} = '$readDateTime' WHERE ${ID.name} = '$id'"
    }
}