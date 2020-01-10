/*
 * Copyright (C) 2019. OpenLattice, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@openlattice.com
 *
 *
 */
package com.openlattice.indexing

import com.google.common.base.Stopwatch
import com.google.common.util.concurrent.ListenableFuture
import com.google.common.util.concurrent.ListeningExecutorService
import com.hazelcast.core.*
import com.hazelcast.map.listener.EntryEvictedListener
import com.hazelcast.query.Predicates
import com.hazelcast.spi.exception.DistributedObjectDestroyedException
import com.openlattice.conductor.rpc.ConductorElasticsearchApi
import com.openlattice.data.storage.EntityDatastore
import com.openlattice.data.storage.IndexingMetadataManager
import com.openlattice.data.storage.MetadataOption
import com.openlattice.edm.type.EntityType
import com.openlattice.edm.type.PropertyType
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.hazelcast.HazelcastQueue
import com.openlattice.indexing.configuration.IndexerConfiguration
import com.openlattice.linking.util.PersonProperties
import com.openlattice.postgres.DataTables.*
import com.openlattice.postgres.PostgresArrays
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresTable.IDS
import com.openlattice.postgres.ResultSetAdapters
import com.openlattice.postgres.mapstores.EntityTypeMapstore
import com.openlattice.postgres.streams.BasePostgresIterable
import com.openlattice.postgres.streams.StatementHolderSupplier
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.time.Instant
import java.time.OffsetDateTime
import java.util.*
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit

internal const val LINKING_INDEXING_TIMEOUT_MILLIS = 120_000L // 2 min
internal const val LINKING_INDEX_QUERY_LIMIT = 3000

@Component
class BackgroundLinkingIndexingService(
        hazelcastInstance: HazelcastInstance,
        private val executor: ListeningExecutorService,
        private val hds: HikariDataSource,
        private val elasticsearchApi: ConductorElasticsearchApi,
        private val dataManager: IndexingMetadataManager,
        private val dataStore: EntityDatastore,
        private val indexerConfiguration: IndexerConfiguration
) {

    companion object {
        private val logger = LoggerFactory.getLogger(BackgroundLinkingIndexingService::class.java)
        const val LINKING_INDEX_SIZE = 100
    }

    private val propertyTypes: IMap<UUID, PropertyType> = hazelcastInstance.getMap(HazelcastMap.PROPERTY_TYPES.name)
    private val entityTypes: IMap<UUID, EntityType> = hazelcastInstance.getMap(HazelcastMap.ENTITY_TYPES.name)

    // TODO if at any point there are more linkable entity types, this must change
    private val personEntityType = entityTypes.values(
            Predicates.equal(
                    EntityTypeMapstore.FULLQUALIFIED_NAME_PREDICATE,
                    PersonProperties.PERSON_TYPE_FQN.fullQualifiedNameAsString
            )
    ).first()

    // TODO if at any point there are more linkable entity types, this must change
    private val personPropertyTypes = propertyTypes.getAll(personEntityType.properties)

    private val linkingIndexingLocks: IMap<UUID, Long> = hazelcastInstance.getMap(
            HazelcastMap.LINKING_INDEXING_LOCKS.name
    )

    init {
        linkingIndexingLocks.addEntryListener(EntryEvictedListener<UUID, Long> {
            logger.info(
                    "Linking id ${it.key} with expiration ${it.oldValue} got evicted at ${Instant.now().toEpochMilli()}"
            )
        }, true)
    }

    /**
     * Queue containing linking ids, which need to be re-indexed in elasticsearch.
     */
    private val indexCandidates = hazelcastInstance
            .getQueue<Triple<List<Array<UUID>>, UUID, OffsetDateTime>>(HazelcastQueue.LINKING_INDEXING.name)

    /**
     * Queue containing linking ids, which need to be un-indexed (deleted) from elasticsearch.
     */
    private val unIndexCandidates = hazelcastInstance
            .getQueue<Triple<List<Array<UUID>>, UUID, OffsetDateTime>>(HazelcastQueue.LINKING_UNINDEXING.name)


    @Suppress("UNUSED")
    private val linkingIndexingEnqueueJob = submitEnqueueTask(indexCandidates, true)

    @Suppress("UNUSED")
    private val linkingUnIndexingEnqueueJob = submitEnqueueTask(unIndexCandidates, false)


    private fun submitEnqueueTask(
            candidates: IQueue<Triple<List<Array<UUID>>, UUID, OffsetDateTime>>,
            createMode: Boolean
    ): ListenableFuture<*>? {
        if (!isLinkingIndexingEnabled()) {
            return null
        }
        val taskName = if (createMode) "indexing" else "un-indexing"
        return executor.submit {
            while (true) {
                try {
                    if (createMode) {
                        getDirtyLinkingIds()
                    } else {
                        getDeletedLinkingIds()
                    }
                            .filter { lockOrRefresh(it.second) }
                            .forEach {
                                logger.info("Registering linking id ${it.second} needing $taskName.")
                                candidates.put(it)
                            }


                } catch (ex: Exception) {
                    logger.info("Encountered error while updating candidates for linking $taskName.", ex)
                }
            }
        }
    }


    private val limiter = Semaphore(indexerConfiguration.parallelism)

    @Suppress("UNUSED")
    private val linkingIndexingJob = submitIndexingTask(indexCandidates, true)

    @Suppress("UNUSED")
    private val linkingUnIndexingJob = submitIndexingTask(unIndexCandidates, false)

    private fun submitIndexingTask(
            candidates: IQueue<Triple<List<Array<UUID>>, UUID, OffsetDateTime>>,
            createMode: Boolean
    ): ListenableFuture<*>? {
        if (!isLinkingIndexingEnabled()) {
            return null
        }

        val taskName = if (createMode) "index" else "un-index"
        return executor.submit {
            while (true) {
                try {
                    generateSequence { candidates.poll(10000, TimeUnit.MILLISECONDS) } // wait 10 seconds
                            .chunked(LINKING_INDEX_SIZE)
                            .forEach { candidateBatch ->
                                logger.info("Starting background linking $taskName task for linking ids " +
                                        "${candidateBatch.map { it.second }}.")

                                limiter.acquire()
                                val (linkingEntityKeyIdsWithLastWrite, linkingIds) = mapCandidates(candidateBatch)

                                try {
                                    if (createMode) {
                                        index(linkingEntityKeyIdsWithLastWrite, linkingIds)
                                    } else {
                                        unIndex(linkingEntityKeyIdsWithLastWrite, linkingIds)
                                    }
                                } catch (ex: Exception) {
                                    logger.error("Unable to $taskName from batch of linking ids $linkingIds.", ex)
                                } finally {
                                    unLock(linkingIds)
                                    limiter.release()
                                }
                            }


                } catch (ex: DistributedObjectDestroyedException) {
                    logger.error("Linking $taskName queue destroyed.", ex)
                } catch (ex: Throwable) {
                    logger.error("Encountered error when linking ${taskName}ing.", ex)
                }
            }
        }
    }

    private fun mapCandidates(
            candidates: List<Triple<List<Array<UUID>>, UUID, OffsetDateTime>>
    ): Pair<Map<UUID, Map<UUID, Map<UUID, OffsetDateTime>>>, Set<UUID>> {

        // entity set id -> linking id -> last write
        val linkingEntityKeyIdsWithLastWrite = mutableMapOf<UUID, MutableMap<UUID, MutableMap<UUID, OffsetDateTime>>>()
        val linkingIds = mutableSetOf<UUID>()
        candidates.forEach {
            val linkingId = it.second
            val lastWrite = it.third

            it.first.forEach { entityDataKey ->
                val (entitySetId, originId) = entityDataKey

                linkingEntityKeyIdsWithLastWrite
                        .getOrPut(entitySetId) { mutableMapOf() }
                        .getOrPut(originId) { mutableMapOf() }[linkingId] = lastWrite

                linkingIds.add(linkingId)
            }
        }

        return linkingEntityKeyIdsWithLastWrite to linkingIds
    }

    /**
     * Collect data and indexes linking ids in elasticsearch and marks them as indexed.
     * @param linkingEntityKeyIdsWithLastWrite Map of entity set id -> origin id -> linking id -> last write of linking
     * entity.
     * @param linkingIds The linking ids about to get indexed.
     */
    private fun index(
            linkingEntityKeyIdsWithLastWrite: Map<UUID, Map<UUID, Map<UUID, OffsetDateTime>>>,
            linkingIds: Set<UUID>
    ) {
        logger.info("Starting background linking indexing task for linking ids $linkingIds.")
        val watch = Stopwatch.createStarted()

        // get data for linking id by entity set ids and property ids
        // (normal)entity_set_id/linking_id
        val dirtyLinkingIdsByEntitySetIds = linkingEntityKeyIdsWithLastWrite.keys.associateWith {
            Optional.of(linkingEntityKeyIdsWithLastWrite.values.flatMap { it.values.flatMap { it.keys } }.toSet())
        }
        val propertyTypesOfEntitySets = linkingEntityKeyIdsWithLastWrite.keys.associateWith { personPropertyTypes } // entity_set_id/property_type_id/property_type
        val linkedEntityData = dataStore // linking_id/(normal)entity_set_id/entity_key_id/property_type_id
                .getLinkedEntityDataByLinkingIdWithMetadata(
                        dirtyLinkingIdsByEntitySetIds,
                        propertyTypesOfEntitySets,
                        EnumSet.of(MetadataOption.LAST_WRITE)
                )

        val indexCount = indexLinkedEntities(linkingEntityKeyIdsWithLastWrite, linkedEntityData)

        logger.info(
                "Finished linked indexing $indexCount elements with linking ids $linkingIds in " +
                        "${watch.elapsed(TimeUnit.MILLISECONDS)} ms."
        )
    }

    /**
     * Un-index linking ids, where there are no normal entities left.
     * The documents need to be deleted from elasticsearch and last_index needs to be updated.
     * @param linkingEntityKeyIdsWithLastWrite Map of entity set id -> origin id -> linking id -> last write of linking
     * entity.
     * @param linkingIds The linking ids about to get un-indexed.
     */
    private fun unIndex(
            linkingEntityKeyIdsWithLastWrite: Map<UUID, Map<UUID, Map<UUID, OffsetDateTime>>>,
            linkingIds: Set<UUID>
    ) {
        logger.info("Starting background linking un-indexing task for linking ids $linkingIds.")
        val watch = Stopwatch.createStarted()

        val indexCount = unIndexLinkedEntities(linkingEntityKeyIdsWithLastWrite, linkingIds)

        logger.info(
                "Finished linked un-indexing $indexCount elements with linking ids $linkingIds in " +
                        "${watch.elapsed(TimeUnit.MILLISECONDS)} ms."
        )
    }

    /**
     * @param linkingIdsWithLastWrite Map of entity_set_id -> origin id -> linking_id -> last_write
     * @param dataByLinkingId Map of linking_id -> entity_set_id -> id -> property_type_id -> data
     * @return Returns the number of normal entities that are associated to the linking ids, that got indexed.
     */
    private fun indexLinkedEntities(
            linkingIdsWithLastWrite: Map<UUID, Map<UUID, Map<UUID, OffsetDateTime>>>,
            dataByLinkingId: Map<UUID, Map<UUID, Map<UUID, Map<UUID, Set<Any>>>>>
    ): Int {
        if (!elasticsearchApi.createBulkLinkedData(personEntityType.id, dataByLinkingId)) {
            return 0
        }
        return dataManager.markLinkingEntitiesAsIndexed(linkingIdsWithLastWrite)
    }

    /**
     * @param linkingIdsWithLastWrite Map of entity_set_id -> origin_id -> linking_id -> last_write
     * @param linkingIds Set of linking_ids to delete from elasticsearch.
     * @return Returns the number of normal entities that are associated to the linking ids, that got un-indexed.
     */
    private fun unIndexLinkedEntities(
            linkingIdsWithLastWrite: Map<UUID, Map<UUID, Map<UUID, OffsetDateTime>>>,
            linkingIds: Set<UUID>
    ): Int {
        if (!elasticsearchApi.deleteEntityDataBulk(personEntityType.id, linkingIds)) {
            return 0
        }
        return dataManager.markLinkingEntitiesAsIndexed(linkingIdsWithLastWrite)
    }


    /**
     * Checks if linking id is already "locked" in [linkingIndexingLocks] map and refreshes its expiration.
     * Returns true, if the linking entity is not yet locked to be processed and false otherwise.
     */
    private fun lockOrRefresh(linkingId: UUID): Boolean {
        try {
            linkingIndexingLocks.lock(linkingId)

            val expiration = lockOrGet(linkingId)
            // expiration should be >= now, otherwise entry should be evicted
            if (expiration != null && Instant.now().toEpochMilli() <= expiration) {
                // if it is about to expire (but not yet processed), we refresh the expiration/time to live
                logger.info("Refreshing expiration for linking id {}", linkingId)
                refreshExpiration(linkingId)
            }

            return expiration == null
        } finally {
            linkingIndexingLocks.unlock(linkingId)
        }
    }

    /**
     * @return Null if locked, expiration in millis otherwise.
     */
    private fun lockOrGet(linkingId: UUID): Long? {
        return linkingIndexingLocks.putIfAbsent(
                linkingId,
                Instant.now().plusMillis(LINKING_INDEXING_TIMEOUT_MILLIS).toEpochMilli(),
                LINKING_INDEXING_TIMEOUT_MILLIS,
                TimeUnit.MILLISECONDS
        )
    }

    /**
     * Refreshes expiration and time to live for the specified linking id.
     */
    private fun refreshExpiration(linkingId: UUID) {
        linkingIndexingLocks.set(
                linkingId,
                Instant.now().plusMillis(LINKING_INDEXING_TIMEOUT_MILLIS).toEpochMilli(),
                LINKING_INDEXING_TIMEOUT_MILLIS,
                TimeUnit.MILLISECONDS
        )
    }

    private fun unLock(linkingIds: Collection<UUID>) {
        linkingIds.forEach(linkingIndexingLocks::delete)
    }

    private fun isLinkingIndexingEnabled(): Boolean {
        return indexerConfiguration.backgroundLinkingIndexingEnabled
    }

    /**
     * Returns the linking ids, which are needing to be indexed along with last_write and their entity set ids and
     * origin ids as a list of arrays with 2 elements.
     * Either because of property change or because of partial entity deletion(soft or hard) from the cluster.
     */
    private fun getDirtyLinkingIds(): BasePostgresIterable<Triple<List<Array<UUID>>, UUID, OffsetDateTime>> {
        return BasePostgresIterable(
                StatementHolderSupplier(hds, selectDirtyLinkingIds, FETCH_SIZE)
        ) { rs ->
            val entityDataKeysRaw = PostgresArrays.getUuidArrayOfArrays(rs, ENTITY_DATA_KEY)!!.toList()
            Triple(
                    entityDataKeysRaw,
                    ResultSetAdapters.linkingId(rs),
                    ResultSetAdapters.lastWriteTyped(rs)
            )
        }
    }


    /**
     * Returns the linking ids which are needing to be un-indexed (to delete those documents) along with last_write and
     * their entity set ids and origin ids as a list of arrays with 2 elements.
     */
    private fun getDeletedLinkingIds(): BasePostgresIterable<Triple<List<Array<UUID>>, UUID, OffsetDateTime>> {
        return BasePostgresIterable(
                StatementHolderSupplier(hds, selectDeletedLinkingIds, FETCH_SIZE)
        ) { rs ->
            val entityDataKeysRaw = PostgresArrays.getUuidArrayOfArrays(rs, ENTITY_DATA_KEY)!!.toList()
            Triple(
                    entityDataKeysRaw,
                    ResultSetAdapters.linkingId(rs),
                    ResultSetAdapters.lastWriteTyped(rs)
            )
        }
    }
}

internal const val ENTITY_DATA_KEY = "entity_data_key"

internal val needsLinkingIndexing =
        // @formatter:off
        "${LAST_INDEX.name} >= ${LAST_WRITE.name} AND "+
        "${LAST_LINK.name} >= ${LAST_WRITE.name} AND " +
        "${LAST_LINK_INDEX.name} < ${LAST_WRITE.name} "
        // @formatter:on

internal val needsLinkingUnIndexing =
        // @formatter:off
        "${LAST_INDEX.name} >= ${LAST_WRITE.name} AND "+
        "${LAST_LINK_INDEX.name} < ${LAST_WRITE.name} "
        // @formatter:on

/**
 * Select linking ids, where ALL normal entities are cleared or deleted.
 */
internal val selectDeletedLinkingIds =
        // @formatter:off
        "SELECT " +
            "${LINKING_ID.name}, " +
            "array_agg(ARRAY[${ENTITY_SET_ID.name}, ${ID.name}]) AS $ENTITY_DATA_KEY, " +
            "max(${LAST_WRITE.name}) AS ${LAST_WRITE.name} " +
        "FROM ${IDS.name} " +
        "WHERE " +
            "${LINKING_ID.name} NOT IN ( " +
                "SELECT ${LINKING_ID.name} " +
                "FROM ${IDS.name} " +
                "WHERE " +
                    "${LINKING_ID.name} IS NOT NULL AND " +
                    "${VERSION.name} > 0 " +
                ") AND " +
            "${LINKING_ID.name} IS NOT NULL AND " +
            needsLinkingUnIndexing +
        "GROUP BY ${LINKING_ID.name} " +
        "LIMIT $LINKING_INDEX_QUERY_LIMIT"
        // @formatter:on


internal const val withAlias = "valid_linking_entities"

/**
 * Select linking ids, where both indexing and linking already finished, but linking indexing is due and those where
 * some normal entities are cleared or deleted.
 */
internal val selectDirtyLinkingIds =
        // @formatter:off
        "WITH $withAlias AS " +
        "(" +
            "SELECT ${LINKING_ID.name}, " +
                    "${ID.name}, " +
                    "${ENTITY_SET_ID.name}, " +
                    "${LAST_WRITE.name}, " +
                    "${LAST_INDEX.name}, " +
                    "${LAST_LINK.name}, " +
                    "${LAST_LINK_INDEX.name} " +
            "FROM ${IDS.name} " +
            "WHERE " +
                "${LINKING_ID.name} IS NOT NULL AND " +
                "${VERSION.name} > 0 " +
        ") " +
        "SELECT " +
            "${LINKING_ID.name}, " +
            "array_agg(ARRAY[${ENTITY_SET_ID.name}, ${ID.name}]) AS $ENTITY_DATA_KEY, " +
            "max(${LAST_WRITE.name}) AS ${LAST_WRITE.name} " +
        "FROM $withAlias " +
        "WHERE $needsLinkingIndexing " +
        "GROUP BY ${LINKING_ID.name} " +

        "UNION ALL " +

        "SELECT " +
            "${LINKING_ID.name}, " +
            "array_agg(ARRAY[${ENTITY_SET_ID.name}, ${ID.name}]) AS $ENTITY_DATA_KEY, " +
            "max(${LAST_WRITE.name}) AS ${LAST_WRITE.name} " +
        "FROM ${IDS.name} " +
        "WHERE " +
            "${LINKING_ID.name} IS NOT NULL AND " +
            "${VERSION.name} <= 0 AND " +
            "$needsLinkingUnIndexing AND " +
            "${LINKING_ID.name} IN " +
                "( " +
                    "SELECT ${LINKING_ID.name} " +
                    "FROM $withAlias " +
                ") AND " +
            "${ENTITY_SET_ID.name} IN " +
                "( " +
                    "SELECT ${ENTITY_SET_ID.name} " +
                    "FROM $withAlias " +
                ") " +
        "GROUP BY ${LINKING_ID.name} " +
        "LIMIT $LINKING_INDEX_QUERY_LIMIT"
        // @formatter:on