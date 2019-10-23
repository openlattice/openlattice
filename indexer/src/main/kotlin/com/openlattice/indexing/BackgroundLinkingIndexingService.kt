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

import com.codahale.metrics.annotation.Timed
import com.google.common.base.Stopwatch
import com.google.common.util.concurrent.ListeningExecutorService
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.core.IMap
import com.hazelcast.query.Predicates
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
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresTable.IDS
import com.openlattice.postgres.ResultSetAdapters
import com.openlattice.postgres.mapstores.EntityTypeMapstore
import com.openlattice.postgres.streams.BasePostgresIterable
import com.openlattice.postgres.streams.StatementHolderSupplier
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.time.Instant
import java.time.OffsetDateTime
import java.util.*
import java.util.concurrent.TimeUnit
import kotlin.streams.asStream

internal const val LINKING_INDEXING_TIMEOUT_MILLIS = 120_000L // 2 min
internal const val LINKING_INDEX_RATE = 60_000L // 1 min

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

    /**
     * Queue containing linking ids, which need to be re-indexed in elasticsearch.
     */
    private val candidates = hazelcastInstance.getQueue<Triple<UUID, OffsetDateTime, Set<UUID>>>(
            HazelcastQueue.LINKING_INDEXING.name
    )

    @Suppress("UNUSED")
    private val linkingIndexingWorker = executor.submit {
        generateSequence(candidates::take)
                .chunked(LINKING_INDEX_SIZE)
                .asStream()
                .parallel()
                .forEach { candidateBatch ->
                    if (!indexerConfiguration.backgroundLinkingIndexingEnabled) {
                        return@forEach
                    }

                    // entity set id -> linking id -> last write
                    val linkingEntityKeyIdsWithLastWrite = mutableMapOf<UUID, MutableMap<UUID, OffsetDateTime>>()
                    val linkingIds = mutableSetOf<UUID>()
                    candidateBatch.forEach {
                        val linkingId = it.first
                        val lastWrite = it.second
                        it.third.forEach { entitySetId ->
                            if (!linkingEntityKeyIdsWithLastWrite.containsKey(entitySetId)) {
                                linkingEntityKeyIdsWithLastWrite[entitySetId] = mutableMapOf()
                            }
                            linkingEntityKeyIdsWithLastWrite.getValue(entitySetId)[linkingId] = lastWrite
                        }
                        linkingIds.add(it.first)
                    }

                    try {
                        lock(linkingIds)
                        index(linkingEntityKeyIdsWithLastWrite, linkingIds)
                    } catch (ex: Exception) {
                        logger.error("Unable to index linking entity with from bacth if linking ids $linkingIds.", ex)
                    } finally {
                        unLock(linkingIds)
                    }
                }
    }

    @Timed
    @Suppress("UNUSED")
    @Scheduled(fixedRate = LINKING_INDEX_RATE)
    fun updateCandidateList() {
        if (!indexerConfiguration.backgroundLinkingIndexingEnabled) {
            return
        }
        executor.submit {
            logger.info("Registering linking ids needing indexing.")

            getDirtyLinkingIds().forEach(candidates::put)
        }
    }

    /**
     * Collect data and indexes linking ids in elasticsearch and marks them as indexed.
     * @param linkingEntityKeyIdsWithLastWrite Map of entity set id -> linking id -> last write of linking entity.
     * @param linkingIds The linking ids about to get indexed.
     */
    private fun index(linkingEntityKeyIdsWithLastWrite: Map<UUID, Map<UUID, OffsetDateTime>>, linkingIds: Set<UUID>) {
        logger.info("Starting background linking indexing task for linking ids $linkingIds.")
        val watch = Stopwatch.createStarted()

        // get data for linking id by entity set ids and property ids
        // (normal)entity_set_id/linking_id
        val dirtyLinkingIdsByEntitySetIds = linkingEntityKeyIdsWithLastWrite.keys.associateWith {
            Optional.of(linkingEntityKeyIdsWithLastWrite.values.flatMap { it.keys }.toSet())
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
     * @param linkingIdsWithLastWrite Map of entity_set_id -> linking_id -> last_write
     * @param dataByLinkingId Map of linking_id -> entity_set_id -> id -> property_type_id -> data
     */
    private fun indexLinkedEntities(
            linkingIdsWithLastWrite: Map<UUID, Map<UUID, OffsetDateTime>>,
            dataByLinkingId: Map<UUID, Map<UUID, Map<UUID, Map<UUID, Set<Any>>>>>
    ): Int {
        if (elasticsearchApi.createBulkLinkedData(personEntityType.id, dataByLinkingId)) {
            return 0
        }
        return dataManager.markAsIndexed(linkingIdsWithLastWrite, true)
    }

    private fun lock(linkingIds: Collection<UUID>) {
        linkingIds.forEach { linkingId ->
            val existingExpiration = linkingIndexingLocks.putIfAbsent(
                    linkingId,
                    Instant.now().plusMillis(LINKING_INDEXING_TIMEOUT_MILLIS).toEpochMilli()
            )
            check(existingExpiration == null) {
                "Unable to lock $linkingId. Existing lock expires at $existingExpiration."
            }
        }
    }


    private fun unLock(linkingIds: Collection<UUID>) {
        linkingIds.forEach(linkingIndexingLocks::delete)
    }

    /**
     * Returns the linking ids, which are needing to be indexed along with their last_write and entity sets.
     * Either because of property change or because of partial entity deletion(soft or hard) from the cluster.
     */
    private fun getDirtyLinkingIds(): BasePostgresIterable<Triple<UUID, OffsetDateTime, Set<UUID>>> {
        return BasePostgresIterable(
                StatementHolderSupplier(hds, selectDirtyLinkingIds, FETCH_SIZE)
        ) {
            Triple(
                    ResultSetAdapters.linkingId(it),
                    ResultSetAdapters.lastWriteTyped(it),
                    ResultSetAdapters.entitySetIds(it)
            )
        }
    }


    /**
     * Returns the linking ids along with last_write and its entity set ids, which are needing to be un-indexed
     * (to delete those documents).
     */
    private fun getDeletedLinkingIds(): BasePostgresIterable<Triple<UUID, OffsetDateTime, Set<UUID>>> {
        return BasePostgresIterable(
                StatementHolderSupplier(hds, selectDeletedLinkingIds, FETCH_SIZE)
        ) {
            Triple(
                    ResultSetAdapters.linkingId(it),
                    ResultSetAdapters.lastWriteTyped(it),
                    ResultSetAdapters.entitySetIds(it)
            )
        }

    }
}

/**
 * Select linking ids, where ALL normal entities are cleared or deleted.
 */
internal val selectDeletedLinkingIds =
        // @formatter:off
        "SELECT " +
                "${LINKING_ID.name}, " +
                "max(${LAST_WRITE.name}) AS ${LAST_WRITE.name}, " +
                "array_agg(${ENTITY_SET_ID.name}) AS ${ENTITY_SET_IDS.name} " +
        "FROM ${IDS.name} " +
        "WHERE ${LINKING_ID.name} NOT IN ( " +
                "SELECT ${LINKING_ID.name} " +
                "FROM ${IDS.name} " +
                "WHERE " +
                "${LINKING_ID.name} IS NOT NUL " +
                "${VERSION.name} > 0 " +
        " ) AND ${LINKING_ID.name} IS NOT NULL " +
        "GROUP BY ${LINKING_ID.name}"
        // @formatter:on


internal const val withAlias = "valid_linking_entities"

/**
 * Select linking ids, where both indexing and linking already finished, but linking indexing is due and those where
 * some normal entities are cleared or deleted.
 */
internal val selectDirtyLinkingIds =
        // @formatter:off
        "WITH $withAlias AS " +
            "(SELECT ${LINKING_ID.name}, " +
                    "${ENTITY_SET_ID.name}, " +
                    "${LAST_WRITE.name}, " +
                    "${LAST_INDEX.name}, " +
                    "${LAST_LINK.name}, " +
                    "${LAST_LINK_INDEX.name} " +
            "FROM ${IDS.name} " +
            "WHERE ${LINKING_ID.name} IS NOT NULL " +
                "AND ${VERSION.name} > 0 ) " +
        "SELECT ${LINKING_ID.name}, " +
                "max(${LAST_WRITE.name}) AS ${LAST_WRITE.name}, " +
                "array_agg(${ENTITY_SET_ID.name}) AS ${ENTITY_SET_IDS.name} " +
        "FROM $withAlias " +
        "WHERE ${LAST_INDEX.name} >= ${LAST_WRITE.name} AND " +
              "${LAST_LINK.name} >= ${LAST_WRITE.name} AND " +
              "${LAST_LINK_INDEX.name} < ${LAST_WRITE.name} " +
        "GROUP BY ${LINKING_ID.name} " +
        "UNION ALL " +
        "SELECT ${LINKING_ID.name}, " +
                "max(${LAST_WRITE.name}) AS ${LAST_WRITE.name}, " +
                "array_agg(${ENTITY_SET_ID.name}) AS ${ENTITY_SET_IDS.name} " +
        "FROM ${IDS.name} " +
        "WHERE ${LINKING_ID.name} IN " +
            "( SELECT ${LINKING_ID.name} " +
                "FROM ${IDS.name} " +
                "WHERE " +
                    "${LINKING_ID.name} IS NOT NULL AND " +
                    "${VERSION.name} <= 0 ) " +
            "AND ${LINKING_ID.name} IN " +
                "( SELECT ${LINKING_ID.name} " +
                  "FROM $withAlias ) " +
            "AND ${ENTITY_SET_ID.name} IN " +
                "( SELECT ${ENTITY_SET_ID.name} " +
                  "FROM $withAlias ) " +
            "AND ${LINKING_ID.name} IS NOT NULL " +
        "GROUP BY ${LINKING_ID.name} "
        // @formatter:on