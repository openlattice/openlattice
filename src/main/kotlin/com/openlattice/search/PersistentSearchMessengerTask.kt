package com.openlattice.search

import com.google.common.collect.SetMultimap
import com.openlattice.authorization.*
import com.openlattice.data.requests.NeighborEntityDetails
import com.openlattice.edm.type.PropertyType
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
        val accessChecks = getPropertyTypeIdsForEntitySet(entitySetId).map {
            AccessCheck(
                    AclKey(entitySetId, it), EnumSet.of(Permission.READ)
            )
        }.toMutableSet()
        return getDependency().propertyTypes.getAll(
                getDependency().authorizationManager.accessChecksForPrincipals(accessChecks, principals)
                        .filter { it.permissions[Permission.READ]!! }
                        .map { it.aclKey[1] }
                        .collect(Collectors.toSet()))
    }

    private fun getUpdatedConstraints(persistentSearch: PersistentSearch): SearchConstraints {
        val constraints: SearchConstraints = persistentSearch.searchConstraints
        val timeFilterConstraintGroup = SearchConstraints.writeDateTimeFilterConstraints(
                constraints.entitySetIds,
                constraints.start,
                constraints.maxHits,
                Optional.of(persistentSearch.lastRead),
                Optional.empty()
        ).constraintGroups[0]
        constraints.constraintGroups.add(timeFilterConstraintGroup)
        return constraints
    }

    private fun sendAlertsForNewWrites(
            userSecurablePrincipal: SecurablePrincipal,
            persistentSearch: PersistentSearch,
            newResults: DataSearchResult,
            neighborsById: Map<UUID, List<NeighborEntityDetails>>
    ) {
        val dependencies = getDependency()

        val userEmail = dependencies.principalsManager.getUser(userSecurablePrincipal.principal.id).email
        newResults.hits.forEach {
            val entityKeyId = UUID.fromString(it[DataTables.ID_FQN].first().toString())
            val renderableEmail = PersistentSearchEmailRenderer.renderEmail(
                    persistentSearch, it, userEmail, neighborsById.getOrDefault(
                    entityKeyId, listOf()
            ), dependencies
            )
            dependencies.mailServiceClient.spool(renderableEmail)
        }
    }

    private fun getLatestRead(vehicleReads: List<SetMultimap<FullQualifiedName, Any>>): OffsetDateTime? {
        return vehicleReads
                .flatMap { it[LAST_WRITE_FQN] ?: emptySet() }
                .map {
                    logger.info("New read datetime as string: {}", it)
                    val localDateTime = Timestamp.valueOf(it.toString()).toLocalDateTime()
                    OffsetDateTime.of(localDateTime, ZoneId.systemDefault().rules.getOffset(localDateTime))
                }.max()
    }

    private fun getHitEntityKeyIds(hits: List<SetMultimap<FullQualifiedName, Any>>): Set<UUID> {
        return hits.map { UUID.fromString(it[DataTables.ID_FQN].first().toString()) }.toSet()
    }

    private fun findNewWritesForAlert(userAclKey: AclKey, persistentSearch: PersistentSearch): OffsetDateTime? {
        val dependencies = getDependency()

        val userSecurablePrincipal = dependencies.principalsManager.getSecurablePrincipal(userAclKey)
        val allUserPrincipals = dependencies.principalsManager.getAllPrincipals(
                userSecurablePrincipal
        ).map { it.principal }.toSet().plus(userSecurablePrincipal.principal)

        if (userSecurablePrincipal.principal == null || userSecurablePrincipal.principal.id == null) {
            logger.error(
                    "Failed to send persistent search for unrecognized principal {} with aclKey {}",
                    userSecurablePrincipal, userAclKey
            )
            return null
        }

        val entitySetIds = persistentSearch.searchConstraints.entitySetIds.toSet()
        val authorizedEntitySetIds = dependencies.authorizationHelper
                .getAuthorizedEntitySetsForPrincipals(entitySetIds, EdmAuthorizationHelper.READ_PERMISSION, allUserPrincipals)

        val authorizedPropertyTypesByEntitySet = dependencies.authorizationHelper.getAuthorizedPropertiesOnEntitySets(
                dependencies.entitySets.keys, EdmAuthorizationHelper.READ_PERMISSION, allUserPrincipals)

        val constraints = getUpdatedConstraints(persistentSearch)
        var results = DataSearchResult(0, listOf())
        if (authorizedEntitySetIds.size == entitySetIds.size) {
            results = dependencies.searchService.executeSearch(constraints, authorizedPropertyTypesByEntitySet)
        }

        if (results.numHits > 0) {
            val entitySets = dependencies.entitySets.getAll(entitySetIds).values.groupBy { it.isLinking }
            val neighborsById = mutableMapOf<UUID, List<NeighborEntityDetails>>()

            if (results.hits.isNotEmpty()) neighborsById.putAll(
                    dependencies.searchService.executeEntityNeighborSearch(
                            entitySets.getOrDefault(false, listOf()).map { it.id }.toSet(),
                            EntityNeighborsFilter(getHitEntityKeyIds(results.hits)),
                            allUserPrincipals
                    )
            )
            sendAlertsForNewWrites(userSecurablePrincipal, persistentSearch, results, neighborsById)
            val lastReadDateTime = getLatestRead(results.hits)
            logger.info(
                    "Last read date time {} for alert {} with {} hits", lastReadDateTime, persistentSearch.id,
                    results.numHits)
            return lastReadDateTime
        }

        return null
    }

    override fun runTask() {
        logger.info("Loading new writes for persistent searches and sending alerts")

        val dependencies = getDependency()
        val totalSearchesUpdated = dependencies.hds.connection.use { outConnection ->
            outConnection.prepareStatement(updateLastReadSql()).use { ps ->
                PostgresIterable(Supplier<StatementHolder> {
                    val connection = dependencies.hds.connection
                    val stmt = connection.createStatement()
                    val rs = stmt.executeQuery(LOAD_ACTIVE_ALERTS_SQL)
                    StatementHolder(connection, stmt, rs)
                }, Function<ResultSet, Pair<AclKey, PersistentSearch>> {
                    ResultSetAdapters.aclKey(it) to ResultSetAdapters.persistentSearch(it)
                }).forEach { (aclKey, search) ->
                    val latestRead = findNewWritesForAlert(aclKey, search)
                    if (latestRead != null) {
                        ps.setObject(1, latestRead)
                        ps.setObject(2, search.id)
                        if (logger.isDebugEnabled) {
                            logger.debug("Updating last read for $aclKey and $search")
                        } else {
                            logger.info("Updating last read for $aclKey and ${search.id}")
                        }
                        ps.addBatch()
                    }
                }
                ps.executeBatch().sum()
            }
        }
        logger.info("Updated {} persistent searches.", totalSearchesUpdated)
    }

    private fun updateLastReadSql(): String {
        return "UPDATE ${PERSISTENT_SEARCHES.name} SET ${LAST_READ.name} = ? WHERE ${ID.name} = ?"
    }

    private fun updateLastReadSql(readDateTime: OffsetDateTime, id: UUID): String {
        return "UPDATE ${PERSISTENT_SEARCHES.name} SET ${LAST_READ.name} = '$readDateTime' WHERE ${ID.name} = '$id'"
    }
}