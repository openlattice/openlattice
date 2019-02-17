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
import com.openlattice.postgres.DataTables.*
import com.openlattice.postgres.PostgresArrays
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresTable.IDS
import com.openlattice.postgres.ResultSetAdapters
import com.openlattice.postgres.streams.PostgresIterable
import com.openlattice.postgres.streams.StatementHolder
import com.zaxxer.hikari.HikariDataSource
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import java.sql.ResultSet
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import java.util.function.Function
import java.util.function.Supplier

class BackgroundLinkingIndexingService(
        private val hds: HikariDataSource,
        private val dataStore: EntityDatastore,
        private val elasticsearchApi: ConductorElasticsearchApi,
        private val dataManager: PostgresDataManager,
        hazelcastInstance: HazelcastInstance
) {

    companion object {
        private val logger = LoggerFactory.getLogger(BackgroundLinkingIndexingService::class.java)!!
    }

    private val propertyTypes: IMap<UUID, PropertyType> = hazelcastInstance.getMap(HazelcastMap.PROPERTY_TYPES.name)
    private val entityTypes: IMap<UUID, EntityType> = hazelcastInstance.getMap(HazelcastMap.ENTITY_TYPES.name)
    private val entitySets: IMap<UUID, EntitySet> = hazelcastInstance.getMap(HazelcastMap.ENTITY_SETS.name)

    private val indexingLocks: IMap<UUID, Long> = hazelcastInstance.getMap(HazelcastMap.INDEXING_LOCKS.name)
    private val taskLock = ReentrantLock()


    /**
     * The plan is to collect the "dirty" linking ids, index all the linking entity sets where it is contained and
     * mark it after as indexed.
     */
    @Scheduled(fixedRate = INDEX_RATE)
    fun indexUpdatedLinkedEntities() {
        if (taskLock.tryLock()) {
            try {
                logger.info("Starting background linking indexing task.")

                val linkedEntitySetIdsLookup = entitySets.values // linking_entity_set_id/linked_entity_set_ids
                        .filter(EntitySet::isLinking).map { it.id to it.linkedEntitySets }.toMap()
                val linkedEntitySetIds = linkedEntitySetIdsLookup.values.flatten().toSet()

                // filter which entity set ids have linking entity set ids and only keep those linking ids
                val dirtyLinkingIds = getDirtyLinkingIds().toMap().filterKeys {
                    linkedEntitySetIds.contains(it)
                }.values.flatten().toSet()

                if (!dirtyLinkingIds.isEmpty()) {
                    val watch = Stopwatch.createStarted()

                    val linkingEntitySetIdsByLinkingIds = dirtyLinkingIds.map {
                        //linking_id/linking_entity_set_ids
                        it to dataStore.getLinkingEntitySetIds(it).toHashSet()
                    }.toMap()

                    // get data for each linking id by entity set ids and property ids
                    val dirtyLinkingIdsByEntitySetId = getLinkingIdsByEntitySetIds(
                            dirtyLinkingIds
                    ).toMap() // entity_set_id/linking_id
                    val propertyTypesOfEntitySets = dirtyLinkingIdsByEntitySetId // entity_set_id/property_type_id
                            .map { it.key to getPropertyTypeForEntitySet(it.key) }.toMap()
                    val linkedEntityData = dataStore // linking_id/entity_set_id/property_type_idB
                            .getLinkedEntityDataByLinkingId(dirtyLinkingIdsByEntitySetId, propertyTypesOfEntitySets)


                    // it is important to iterate over linking ids which have an associated linking entity set id!
                    val indexCount = linkingEntitySetIdsByLinkingIds.map {
                        indexLinkedEntity(it.key, it.value, linkedEntityData[it.key]!!, linkedEntitySetIdsLookup)
                    }.sum()

                    logger.info("Created {} linked indices in {} ms", indexCount, watch.elapsed(TimeUnit.MILLISECONDS))
                }
            } finally {
                taskLock.unlock()
            }
        } else {
            logger.info("Not starting new indexing job as an existing one is running.")
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
                linkingEntitySetIds
        )

        val watch = Stopwatch.createStarted()
        var indexCount = 0

        if (linkingEntitySetIds.all {
                    val linkedEntitySetIds = linkedEntitySetIdsLookup[it]!!
                    val filteredData = dataByEntitySetId
                            .filterKeys { entitySetId -> linkedEntitySetIds.contains(entitySetId) }
                    elasticsearchApi.createBulkLinkedData(it, mapOf(linkingId to filteredData))
                }) {
            indexCount += dataManager.markAsIndexed(
                    linkingEntitySetIds.flatMap { linkedEntitySetIdsLookup[it]!! }
                            .map { it to Optional.of(setOf(linkingId)) }.toMap(),
                    true
            )
        }


        logger.info(
                "Finished linked indexing {} elements with linking id {} in {} ms",
                indexCount,
                linkingId,
                watch.elapsed(TimeUnit.MILLISECONDS)
        )

        return indexCount
    }

    /**
     * Returns the linking id, which need to be indexed mapped by their entity set ids.
     * Note: it only returns those entity set ids, where a dirty linking id is present
     */
    private fun getDirtyLinkingIds(): PostgresIterable<Pair<UUID, Set<UUID>>> {
        return PostgresIterable(Supplier<StatementHolder> {
            val connection = hds.connection
            val stmt = connection.prepareStatement(selectDirtyLinkingIds())
            val rs = stmt.executeQuery()
            StatementHolder(connection, stmt, rs)
        }, Function<ResultSet, Pair<UUID, Set<UUID>>> {
            ResultSetAdapters.entitySetId(it) to ResultSetAdapters.linkingIds(it)
        })
    }

    private fun selectDirtyLinkingIds(): String {
        return "SELECT ${ENTITY_SET_ID.name}, array_agg(${LINKING_ID.name}) AS ${LINKING_ID.name} " +
                "FROM ${IDS.name} " +
                "WHERE ${LINKING_ID.name} IS NOT NULL " +
                "AND ${LAST_INDEX.name} >= ${LAST_WRITE.name} " +
                "AND ${LAST_LINK.name} >= ${LAST_WRITE.name} " +
                "AND ${LAST_LINK_INDEX.name} < ${LAST_WRITE.name} " +
                "GROUP BY ${ENTITY_SET_ID.name} " +
                "LIMIT $FETCH_SIZE"
    }

    private fun getLinkingIdsByEntitySetIds(linkingIds: Set<UUID>): PostgresIterable<Pair<UUID, Optional<Set<UUID>>>> {
        return PostgresIterable(Supplier<StatementHolder> {
            val connection = hds.connection
            val stmt = connection.prepareStatement(selectLinkingIdsByEntitySetIds())
            val arr = PostgresArrays.createUuidArray(connection, linkingIds)
            stmt.setArray(1, arr)
            val rs = stmt.executeQuery()
            StatementHolder(connection, stmt, rs)
        }, Function<ResultSet, Pair<UUID, Optional<Set<UUID>>>> {
            ResultSetAdapters.entitySetId(it) to Optional.of(ResultSetAdapters.linkingIds(it))
        })
    }

    private fun selectLinkingIdsByEntitySetIds(): String {
        return "SELECT ${ENTITY_SET_ID.name}, array_agg(${LINKING_ID.name}) AS ${LINKING_ID.name} FROM ${IDS.name} " +
                "WHERE ${LINKING_ID.name} in (SELECT UNNEST( (?)::uuid[] )) " +
                "GROUP BY ${ENTITY_SET_ID.name}"
    }

    private fun getPropertyTypeForEntitySet(entitySetId: UUID): Map<UUID, PropertyType> {
        val entityTypeId = entityTypes[entitySets[entitySetId]?.entityTypeId]!!.id
        return propertyTypes
                .getAll(entityTypes[entityTypeId]?.properties ?: setOf())
                .filter { it.value.datatype != EdmPrimitiveTypeKind.Binary }
    }
}