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
import com.openlattice.postgres.PostgresArrays
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresTable.IDS
import com.openlattice.postgres.ResultSetAdapters
import com.openlattice.postgres.mapstores.EntityTypeMapstore
import com.openlattice.postgres.streams.*
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.time.Instant
import java.time.OffsetDateTime
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.stream.Stream

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
        private val indexerConfiguration: IndexerConfiguration) {

    companion object {
        private val logger = LoggerFactory.getLogger(BackgroundLinkingIndexingService::class.java)
        const val LINKING_INDEX_SIZE = 100
    }

    private val propertyTypes: IMap<UUID, PropertyType> = hazelcastInstance.getMap(HazelcastMap.PROPERTY_TYPES.name)
    private val entityTypes: IMap<UUID, EntityType> = hazelcastInstance.getMap(HazelcastMap.ENTITY_TYPES.name)

    // TODO if at any point there are more linkable entity types, this must change
    private val personEntityType = entityTypes.values(
            Predicates.equal(EntityTypeMapstore.FULLQUALIFIED_NAME_PREDICATE, PersonProperties.PERSON_TYPE_FQN.fullQualifiedNameAsString)
    ).first()

    // TODO if at any point there are more linkable entity types, this must change
    private val personPropertyTypes = propertyTypes.getAll(personEntityType.properties)

    private val linkingIndexingLocks: IMap<UUID, Long> = hazelcastInstance.getMap(HazelcastMap.LINKING_INDEXING_LOCKS.name)

    /**
     * Queue containing linking ids, which need to be re-indexed in elasticsearch.
     */
    private val candidates = hazelcastInstance.getQueue<Pair<UUID, OffsetDateTime>>(HazelcastQueue.LINKING_INDEXING.name)

    init {
        hazelcastInstance.config.getQueueConfig(HazelcastQueue.LINKING_INDEXING.name).maxSize = FETCH_SIZE
    }

    @Suppress("UNUSED")
    private val linkingIndexingWorker = executor.submit {
        Stream.generate {
            val batch = mutableMapOf<UUID, OffsetDateTime>()
            while (candidates.peek() != null && batch.size < LINKING_INDEX_SIZE) {
                val candidate = candidates.poll()
                batch[candidate.first] = candidate.second
            }
            batch
        }
                .parallel()
                .forEach { candidateBatch ->
                    if (!indexerConfiguration.backgroundLinkingIndexingEnabled) {
                        return@forEach
                    }
                    try {
                        lock(candidateBatch.keys)
                        index(candidateBatch)
                    } catch (ex: Exception) {
                        logger.error(
                                "Unable to index linking entity with from bacth if linking ids ${candidateBatch.keys}.",
                                ex
                        )
                    } finally {
                        unLock(candidateBatch.keys)
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

    private fun index(linkingIdsWithLastWrite: Map<UUID, OffsetDateTime>) {
        logger.info("Starting background linking indexing task for linking ids ${linkingIdsWithLastWrite.keys}.")
        val watch = Stopwatch.createStarted()

        // get data for linking id by entity set ids and property ids
        val dirtyLinkingIdsByEntitySetIds = getEntitySetIdsOfLinkingIds(linkingIdsWithLastWrite.keys).toMap() // (normal)entity_set_id/linking_id
        val propertyTypesOfEntitySets = dirtyLinkingIdsByEntitySetIds.keys.associateWith { personPropertyTypes } // entity_set_id/property_type_id/property_type
        val linkedEntityData = dataStore // linking_id/(normal)entity_set_id/entity_key_id/property_type_id
                .getLinkedEntityDataByLinkingIdWithMetadata(
                        dirtyLinkingIdsByEntitySetIds,
                        propertyTypesOfEntitySets,
                        EnumSet.of(MetadataOption.LAST_WRITE)
                )

        val indexCount = indexLinkedEntities(linkingIdsWithLastWrite, linkedEntityData, dirtyLinkingIdsByEntitySetIds)

        logger.info("Finished linked indexing $indexCount elements with linking ids ${linkingIdsWithLastWrite.keys} " +
                "in ${watch.elapsed(TimeUnit.MILLISECONDS)} ms")
    }

    private fun indexLinkedEntities(
            linkingIdsWithLastWrite: Map<UUID, OffsetDateTime>,
            dataByLinkingId: Map<UUID, Map<UUID, Map<UUID, Map<UUID, Set<Any>>>>>,
            linkingIdsByEntitySetIds: Map<UUID, Optional<Set<UUID>>>
    ): Int {
        if (elasticsearchApi.createBulkLinkedData(personEntityType.id, dataByLinkingId)) {
            return 0
        }
        return dataManager.markAsIndexed(
                linkingIdsByEntitySetIds.mapValues {
                    // it should be present, since we use array_agg() + we filter on linking id in query
                    it.value.get().associateWith { linkingId ->
                        linkingIdsWithLastWrite.getValue(linkingId)
                    }
                },
                true
        )
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
     * Returns the linking ids, which are needing to be indexed.
     */
    private fun getDirtyLinkingIds(): BasePostgresIterable<Pair<UUID, OffsetDateTime>> {
        return BasePostgresIterable(
                StatementHolderSupplier(hds, selectDirtyLinkingIds(), FETCH_SIZE)
        ) { rs -> ResultSetAdapters.linkingId(rs) to ResultSetAdapters.lastWriteTyped(rs) }
    }

    private fun selectDirtyLinkingIds(): String {
        // @formatter:off
        return "SELECT ${LINKING_ID.name}, ${LAST_WRITE.name} " +
                "FROM ${IDS.name} " +
                "WHERE " +
                    "${LAST_INDEX.name} >= ${LAST_WRITE.name} AND " +
                    "${LAST_LINK.name} >= ${LAST_WRITE.name} AND " +
                    "${LAST_LINK_INDEX.name} < ${LAST_WRITE.name} AND " +
                    "${VERSION.name} > 0 AND " +
                    "${LINKING_ID.name} IS NOT NULL"
        // @formatter:on
    }

    private fun getEntitySetIdsOfLinkingIds(
            linkingIds: Set<UUID>
    ): BasePostgresIterable<Pair<UUID, Optional<Set<UUID>>>> {
        return BasePostgresIterable(
                PreparedStatementHolderSupplier(hds, selectLinkingIdsByEntitySetIds()) { ps ->
                    val linkingIdsArr = PostgresArrays.createUuidArray(ps.connection, linkingIds)
                    ps.setArray(1, linkingIdsArr)
                }) { ResultSetAdapters.entitySetId(it) to Optional.of(ResultSetAdapters.entityKeyIds(it)) }
    }

    private fun selectLinkingIdsByEntitySetIds(): String {
        return "SELECT ${ENTITY_SET_ID.name}, array_agg(${LINKING_ID.name}) as ${ENTITY_KEY_IDS_COL.name} " +
                "FROM ${IDS.name} " +
                "WHERE ${LINKING_ID.name} = ANY(?) "
    }
}