package com.openlattice.search

import com.hazelcast.query.Predicates
import com.openlattice.authorization.*
import com.openlattice.authorization.securable.SecurableObjectType
import com.openlattice.authorization.util.getLastAclKeySafely
import com.openlattice.data.requests.NeighborEntityDetails
import com.openlattice.edm.EdmConstants
import com.openlattice.edm.EntitySet
import com.openlattice.edm.set.EntitySetFlag
import com.openlattice.edm.type.PropertyType
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresTable.PERSISTENT_SEARCHES
import com.openlattice.postgres.ResultSetAdapters
import com.openlattice.postgres.mapstores.EntitySetMapstore
import com.openlattice.postgres.streams.BasePostgresIterable
import com.openlattice.postgres.streams.StatementHolderSupplier
import com.openlattice.search.requests.DataSearchResult
import com.openlattice.search.requests.EntityNeighborsFilter
import com.openlattice.search.requests.PersistentSearch
import com.openlattice.search.requests.SearchConstraints
import com.openlattice.tasks.HazelcastFixedRateTask
import com.openlattice.tasks.HazelcastTaskDependencies
import com.openlattice.tasks.Task
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.LoggerFactory
import java.time.OffsetDateTime
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.stream.Collectors
import kotlin.streams.asSequence

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
            val entityKeyId = UUID.fromString(it.getValue(EdmConstants.ID_FQN).first().toString())
            val renderableEmail = PersistentSearchEmailRenderer.renderEmail(
                    persistentSearch, it, userEmail, neighborsById.getOrDefault(
                    entityKeyId, listOf()
            ), dependencies
            )
            dependencies.mailServiceClient.spool(renderableEmail)
        }
    }

    private fun getLatestRead(vehicleReads: List<Map<FullQualifiedName, Set<Any>>>): OffsetDateTime? {
        return vehicleReads
                .flatMap { it[EdmConstants.LAST_WRITE_FQN] ?: emptySet() }
                .map { it as OffsetDateTime }.max()
    }

    private fun getHitEntityKeyIds(hits: List<Map<FullQualifiedName, Set<Any>>>): Set<UUID> {
        return hits.map { UUID.fromString((it[EdmConstants.ID_FQN] ?: emptySet()).first().toString()) }.toSet()
    }

    private fun findNewWritesForAlerts(userAclKey: AclKey, persistentSearches: Set<PersistentSearch>): Map<UUID, OffsetDateTime> {
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
            return mapOf()
        }

        val allEntitySetIds = persistentSearches.flatMap { it.searchConstraints.entitySetIds.toSet() }.toSet()

        val authorizedEntitySetIds = dependencies.authorizationHelper
                .getAuthorizedEntitySetsForPrincipals(
                        allEntitySetIds, EdmAuthorizationHelper.READ_PERMISSION, allUserPrincipals
                )

        val authorizedPropertyTypesByEntitySet = dependencies.authorizationHelper.getAuthorizedPropertiesOnEntitySets(
                authorizedEntitySetIds, EdmAuthorizationHelper.READ_PERMISSION, allUserPrincipals
        )

        val updatedReadDateTimes = mutableMapOf<UUID, OffsetDateTime>()

        persistentSearches.forEach { persistentSearch ->

            val entitySetIds = persistentSearch.searchConstraints.entitySetIds.toSet()

            val constraints = getUpdatedConstraints(persistentSearch)

            if (!authorizedEntitySetIds.containsAll(entitySetIds)) {
                return@forEach
            }

            val results = dependencies.searchService.executeSearch(constraints, authorizedPropertyTypesByEntitySet.filterKeys { entitySetIds.contains(it) })

            if (results.hits.isEmpty()) {
                return@forEach
            }

            val entitySets = dependencies.entitySets.getAll(entitySetIds).values.groupBy { it.isLinking }
            val neighborsById = dependencies.searchService.executeEntityNeighborSearch(
                    entitySets.getOrDefault(false, listOf()).map { it.id }.toSet(),
                    EntityNeighborsFilter(
                            getHitEntityKeyIds(results.hits),
                            Optional.empty(),
                            Optional.empty(),
                            Optional.of(getAuthorizedAssociationEntitySets(allUserPrincipals))
                    ),
                    allUserPrincipals
            )
            sendAlertsForNewWrites(userSecurablePrincipal, persistentSearch, results, neighborsById)
            val lastReadDateTime = getLatestRead(results.hits)
            logger.info(
                    "Last read date time {} for alert {} with {} hits", lastReadDateTime, persistentSearch.id,
                    results.numHits
            )
            lastReadDateTime?.let { updatedReadDateTimes[persistentSearch.id] = it }
        }

        return updatedReadDateTimes
    }

    private fun getAuthorizedAssociationEntitySets(principals: Set<Principal>): Set<UUID> {
        val dependencies = getDependency()

        val readableEntitySetIds = dependencies.authorizationManager.getAuthorizedObjectsOfType(
                principals,
                SecurableObjectType.EntitySet,
                EnumSet.of(Permission.READ)
        ).asSequence().map { getLastAclKeySafely(it) }.toSet()

        return dependencies.entitySets.keySet(Predicates.and(
                Predicates.`in`<UUID, EntitySet>(EntitySetMapstore.ID_INDEX, *readableEntitySetIds.toTypedArray()),
                Predicates.equal<UUID, EntitySet>(EntitySetMapstore.FLAGS_INDEX, EntitySetFlag.ASSOCIATION),
                Predicates.notEqual<UUID, EntitySet>(EntitySetMapstore.FLAGS_INDEX, EntitySetFlag.AUDIT)
        ))
    }

    override fun runTask() {

        logger.info("Loading new writes for persistent searches and sending alerts")

        val dependencies = getDependency()

        val persistentSearchesById = BasePostgresIterable(
                StatementHolderSupplier(dependencies.hds, LOAD_ACTIVE_ALERTS_SQL, 32_000)
        ) { ResultSetAdapters.aclKey(it) to ResultSetAdapters.persistentSearch(it) }
                .groupBy { (aclKey, _) -> aclKey }
                .mapValues { it.value.map { (_, persistentSearch) -> persistentSearch }.toSet() }

        logger.info("Loaded {} active persistent searches.", persistentSearchesById.size)

        val lastWritesForMessagesSent = mutableMapOf<UUID, OffsetDateTime>()
        persistentSearchesById.map { (aclKey, searches) ->
            lastWritesForMessagesSent.putAll(findNewWritesForAlerts(aclKey, searches))
        }

        logger.info("Sent {} notifications for persistent searches.", lastWritesForMessagesSent.size)

        val totalSearchesUpdated = dependencies.hds.connection.use { connection ->

            connection.prepareStatement(updateLastReadSql()).use { ps ->

                lastWritesForMessagesSent.forEach { (id, latestRead) ->
                    ps.setObject(1, latestRead)
                    ps.setObject(2, id)
                    ps.addBatch()

                    logger.info("Updating last read for $id")
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