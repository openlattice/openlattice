package com.openlattice.indexing

import com.google.common.base.Stopwatch
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.core.IMap
import com.openlattice.conductor.rpc.ConductorElasticsearchApi
import com.openlattice.data.EntityDatastore
import com.openlattice.data.storage.PostgresDataManager
import com.openlattice.edm.EntitySet
import com.openlattice.edm.type.EntityType
import com.openlattice.edm.type.PropertyType
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.postgres.*
import com.openlattice.postgres.DataTables.*
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.streams.PostgresIterable
import com.openlattice.postgres.streams.StatementHolder
import com.zaxxer.hikari.HikariDataSource
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import java.sql.ResultSet
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.function.Function
import java.util.function.Supplier
import kotlin.collections.HashMap
import com.openlattice.postgres.PostgresTable.IDS

class BackgroundLinkingIndexingService(
        private val hds: HikariDataSource,
        private val dataStore: EntityDatastore,
        private val elasticsearchApi: ConductorElasticsearchApi,
        private val dataManager: PostgresDataManager,
        hazelcastInstance: HazelcastInstance) {

    companion object {
        private val logger = LoggerFactory.getLogger(BackgroundLinkingIndexingService::class.java)!!
    }

    private val propertyTypes: IMap<UUID, PropertyType> = hazelcastInstance.getMap(HazelcastMap.PROPERTY_TYPES.name)
    private val entityTypes: IMap<UUID, EntityType> = hazelcastInstance.getMap(HazelcastMap.ENTITY_TYPES.name)
    private val entitySets: IMap<UUID, EntitySet> = hazelcastInstance.getMap(HazelcastMap.ENTITY_SETS.name)

    /**
     * The plan is to collect the "dirty" linking ids, index all the linking entity sets where it is contained and
     * mark it after as indexed.
     */
    @Scheduled(fixedRate = INDEX_RATE)
    fun indexUpdatedLinkedEntities() {
        logger.info("Starting background linking indexing task.")

        val dirtyLinkingIdsByEntitySetId = getDirtyLinkingIds().toMap() // entity_setId/linkingId

        if (!dirtyLinkingIdsByEntitySetId.isEmpty()) {
            val watch = Stopwatch.createStarted()

            val linkingEntitySetIdsByLinkingIds = HashMap<UUID, HashSet<UUID>>() //linking_id/linking_entity_set_ids
            dirtyLinkingIdsByEntitySetId.forEach { (entitySetId, linkingIds) ->
                linkingIds.forEach {
                    val linkingEntitySetIds = dataStore.getLinkingEntitySetIds(entitySetId).toHashSet()
                    if (!linkingEntitySetIds.isEmpty()) {
                        linkingEntitySetIdsByLinkingIds.putIfAbsent(it, linkingEntitySetIds)?.addAll(linkingEntitySetIds)
                    }
                }
            }

            val linkedEntitySetIdsLookup = entitySets.getAll(linkingEntitySetIdsByLinkingIds.values.flatten().toSet())
                    .mapValues { it.value.linkedEntitySets } // linking_entity_set_id/linked_entity_set_ids

            // Linking ids are not filtered here, whether they need to be indexed or not
            val propertyTypesOfEntitySets = dirtyLinkingIdsByEntitySetId
                    .map { it.key to getPropertyTypeForEntitySet(it.key) }.toMap()
            val linkedEntityData = dataStore.getLinkedEntityDataByLinkingId(
                    dirtyLinkingIdsByEntitySetId.map { it.key to Optional.of(it.value) }.toMap(),
                    propertyTypesOfEntitySets) // linking_id/entity_set_id/property_type_id

            // it is important to iterate over linking ids which have an associated linking entity set id!
            val indexCount = linkingEntitySetIdsByLinkingIds.map {
                indexLinkedEntity(it.key, it.value, linkedEntityData[it.key]!!, linkedEntitySetIdsLookup)
            }.sum()

            logger.info("Created {} linked indices in {} ms", indexCount, watch.elapsed(TimeUnit.MILLISECONDS))
        }

        logger.info("Background linking indexing task finished")
    }

    private fun indexLinkedEntity(
            linkingId: UUID,
            linkingEntitySetIds: Set<UUID>,
            dataByEntitySetId: Map<UUID, Map<UUID, Set<Any>>>,
            linkedEntitySetIdsLookup: Map<UUID, Set<UUID>>
    ): Int {
        logger.info(
                "Starting linked indexing with linking id {} for linking entity sets {}",
                linkingId,
                linkingEntitySetIds)

        val watch = Stopwatch.createStarted()
        var indexCount = 0

        if (linkingEntitySetIds.all {
                    val linkedEntitySetIds = linkedEntitySetIdsLookup[it]!!
                    val filteredData = dataByEntitySetId
                            .filterKeys { entitySetId -> linkedEntitySetIds.contains(entitySetId) }
                    elasticsearchApi.createBulkEntityData(it,  filteredData.mapValues { mapOf(linkingId to it.value) }, true)
                }) {
            indexCount += dataManager.markAsIndexed(
                    linkingEntitySetIds.flatMap{ linkedEntitySetIdsLookup[it]!! }
                            .map{ it to Optional.of(setOf(linkingId)) }.toMap(),
                    true)
        }


        logger.info(
                "Finished linked indexing {} elements with linking id {} in {} ms",
                indexCount,
                linkingId,
                watch.elapsed(TimeUnit.MILLISECONDS))

        return indexCount
    }

    /**
     * Returns the linking id, which need to be indexed by mapped by their entity set ids.
     * Note: the query itself has duplicate linking ids within 1 entity set id, but the
     * [com.openlattice.postgres.ResultSetAdapters.linkingIds] function copies that array into a set.
     */
    private fun getDirtyLinkingIds(): PostgresIterable<Pair<UUID, Set<UUID>>> {
        return PostgresIterable(Supplier<StatementHolder> {
            val connection = hds.connection
            val stmt = connection.prepareStatement(getDirtyLinkingIdsQuery())
            val rs = stmt.executeQuery()
            StatementHolder(connection, stmt, rs)
        }, Function<ResultSet, Pair<UUID, Set<UUID>>> {
            ResultSetAdapters.entitySetId(it) to ResultSetAdapters.linkingIds(it)
        })
    }

    private fun getDirtyLinkingIdsQuery(): String {
        val selectDirtyLinkingIds = selectDirtyLinkingIds()
        return "SELECT ${ENTITY_SET_ID.name}, array_agg(${LINKING_ID.name}) FROM ${IDS.name} " +
                "INNER JOIN " +
                "($selectDirtyLinkingIds) as dirty_ids " +
                "USING(${LINKING_ID.name}) " +
                "GROUP BY ${ENTITY_SET_ID.name}"
    }

    private fun selectDirtyLinkingIds(): String {
        return "SELECT DISTINCT ${LINKING_ID.name} " +
                "FROM ${IDS.name} " +
                "WHERE ${LINKING_ID.name} IS NOT NULL " +
                "AND ${LAST_INDEX.name} >= ${LAST_WRITE.name} " +
                "AND ${LAST_LINK.name} >= ${LAST_WRITE.name} " +
                "AND ${LAST_LINK_INDEX.name} < ${LAST_WRITE.name} " +
                "LIMIT $FETCH_SIZE"
    }

    private fun getPropertyTypeForEntitySet(entitySetId: UUID): Map<UUID, PropertyType> {
        val entityTypeId = entityTypes[entitySets[entitySetId]?.entityTypeId]!!.id
        return propertyTypes
                .getAll(entityTypes[entityTypeId]?.properties ?: setOf())
                .filter { it.value.datatype != EdmPrimitiveTypeKind.Binary }
    }
}