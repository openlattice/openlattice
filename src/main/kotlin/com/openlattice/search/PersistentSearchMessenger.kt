package com.openlattice.search

import com.google.common.collect.SetMultimap
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.core.IMap
import com.openlattice.authorization.*
import com.openlattice.edm.EntitySet
import com.openlattice.edm.type.EntityType
import com.openlattice.edm.type.PropertyType
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.mail.MailServiceClient
import com.openlattice.organizations.roles.SecurePrincipalsManager
import com.openlattice.postgres.DataTables.LAST_WRITE_FQN
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresTable.PERSISTENT_SEARCHES
import com.openlattice.postgres.ResultSetAdapters
import com.openlattice.postgres.streams.PostgresIterable
import com.openlattice.postgres.streams.StatementHolder
import com.openlattice.search.requests.*
import com.zaxxer.hikari.HikariDataSource
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import java.sql.ResultSet
import java.sql.Statement
import java.sql.Timestamp
import java.time.OffsetDateTime
import java.time.ZoneId
import java.time.ZoneOffset
import java.util.*
import java.util.function.Function
import java.util.function.Supplier
import java.util.stream.Collectors

private val logger = LoggerFactory.getLogger(PersistentSearchMessenger::class.java)

const val ALERT_MESSENGER_INTERVAL_MILLIS = 60000L

private val LOAD_ACTIVE_ALERTS_SQL = "SELECT * FROM ${PERSISTENT_SEARCHES.name} WHERE ${EXPIRATION_DATE.name} > now()"

class PersistentSearchMessenger : Runnable {

    private val propertyTypes: IMap<UUID, PropertyType> = PersistentSearchMessengerHelpers.hazelcastInstance.getMap(HazelcastMap.PROPERTY_TYPES.name)
    private val entityTypes: IMap<UUID, EntityType> = PersistentSearchMessengerHelpers.hazelcastInstance.getMap(HazelcastMap.ENTITY_TYPES.name)
    private val entitySets: IMap<UUID, EntitySet> = PersistentSearchMessengerHelpers.hazelcastInstance.getMap(HazelcastMap.ENTITY_SETS.name)

    private fun getPropertyTypeIdsForEntitySet(entitySetId: UUID): Set<UUID> {
        return entityTypes[entitySets[entitySetId]?.entityTypeId]?.properties.orEmpty()
    }

    private fun getAuthorizedPropertyMap(principals: Set<Principal>, entitySetId: UUID): Map<UUID, PropertyType> {

        val accessChecks = getPropertyTypeIdsForEntitySet(entitySetId).map { AccessCheck(AclKey(entitySetId, it), EnumSet.of(Permission.READ)) }.toMutableSet()
        return propertyTypes.getAll(PersistentSearchMessengerHelpers.authorizationManager.accessChecksForPrincipals(accessChecks, principals)
                .filter { it.permissions[Permission.READ]!! }
                .map { it.aclKey[1] }
                .collect(Collectors.toSet()))
    }

    private fun getAuthorizedPropertyTypeMap(securablePrincipal: SecurablePrincipal, entitySetIds: List<UUID>): Map<UUID, Map<UUID, PropertyType>> {
        var principals = PersistentSearchMessengerHelpers.principalsManager.getAllPrincipals(securablePrincipal)
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

    private fun sendAlertsForNewWrites(userSecurablePrincipal: SecurablePrincipal, persistentSearch: PersistentSearch, newResults: DataSearchResult) {
        val userEmail = PersistentSearchMessengerHelpers.principalsManager.getUser(userSecurablePrincipal.principal.id).email
        newResults.hits.forEach {
            val renderableEmail = PersistentSearchEmailRenderer.renderEmail(persistentSearch, it, userEmail)
            PersistentSearchMessengerHelpers.mailServiceClient.spool(renderableEmail)
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

    private fun findNewWritesForAlert(userAclKey: AclKey, persistentSearch: PersistentSearch): OffsetDateTime? {
        val userSecurablePrincipal = PersistentSearchMessengerHelpers.principalsManager.getSecurablePrincipal(userAclKey)
        val entitySets = entitySets.getAll(persistentSearch.searchConstraints.entitySetIds.toSet()).values
                .groupBy { it.isLinking }

        val authorizedPropertyTypeMap = getAuthorizedPropertyTypeMap(userSecurablePrincipal,
                entitySets[false]!!.map { it.id })
        val linkedAuthorizedPropertyTypeMap = getAuthorizedPropertyTypeMap(userSecurablePrincipal,
                entitySets[false]!!.map { it.id })
        val constraints = getUpdatedConstraints(persistentSearch)

        val results = PersistentSearchMessengerHelpers.searchService.executeSearch(constraints,
                authorizedPropertyTypeMap, false)
        val linkedResults = PersistentSearchMessengerHelpers.searchService.executeSearch(constraints,
                linkedAuthorizedPropertyTypeMap, true)
        val newResults = DataSearchResult(results.numHits + linkedResults.numHits,
                results.hits + linkedResults.hits )

        if (newResults.numHits > 0) {
            sendAlertsForNewWrites(userSecurablePrincipal, persistentSearch, newResults)
            return getLatestRead(newResults.hits)
        }

        return null
    }

    private fun updateLastReadForAlert(alertId: UUID, readDateTime: OffsetDateTime) {
        val connection = PersistentSearchMessengerHelpers.hds.connection
        connection.use {
            val stmt: Statement = connection.createStatement()
            stmt.execute(updateLastReadSql(readDateTime, alertId))
        }
    }

    override fun run() {
        logger.info("Loading new writes for persistent searches and sending alerts")

        PostgresIterable(Supplier<StatementHolder> {
            val connection = PersistentSearchMessengerHelpers.hds.connection
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