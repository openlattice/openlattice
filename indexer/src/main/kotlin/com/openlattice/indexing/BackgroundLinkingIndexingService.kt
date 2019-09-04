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
import com.google.common.base.Supplier
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
import com.openlattice.postgres.DataTables
import com.openlattice.postgres.DataTables.*
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresTable.IDS
import com.openlattice.postgres.ResultSetAdapters
import com.openlattice.postgres.mapstores.EntityTypeMapstore
import com.openlattice.postgres.streams.PostgresIterable
import com.openlattice.postgres.streams.StatementHolder
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.sql.ResultSet
import java.time.Instant
import java.time.OffsetDateTime
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.function.Function
import java.util.stream.Stream
import kotlin.system.exitProcess

internal const val LINKING_INDEXING_TIMEOUT_MILLIS = 120_000L
internal const val LINKING_INDEX_RATE = 30_000L

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
    }

    private val propertyTypes: IMap<UUID, PropertyType> = hazelcastInstance.getMap(HazelcastMap.PROPERTY_TYPES.name)
    private val entityTypes: IMap<UUID, EntityType> = hazelcastInstance.getMap(HazelcastMap.ENTITY_TYPES.name)

    // TODO if at any point there are more linkable entity types, this must change
    private val personEntityType = entityTypes.values(
            Predicates.equal(EntityTypeMapstore.FULLQUALIFIED_NAME_PREDICATE, PersonProperties.PERSON_TYPE_FQN.fullQualifiedNameAsString)
    ).first()

    private val personPropertyTypes = propertyTypes.getAll(personEntityType.properties)

    private val linkingIndexingLocks: IMap<UUID, Long> = hazelcastInstance.getMap(HazelcastMap.LINKING_INDEXING_LOCKS.name)

    /**
     * Queue containing linking ids, which need to be re-indexed in elasticsearch.
     */
    private val candidates = hazelcastInstance.getQueue<Pair<UUID, OffsetDateTime>>(HazelcastQueue.LINKING_INDEXING.name)

    @Suppress("UNUSED")
    private val linkingIndexingWorker = executor.submit {
        Stream.generate { candidates.take() }
                .parallel()
                .forEach { candidate ->
                    if (indexerConfiguration.backgroundLinkingIndexingEnabled) {
                        try {
                            lock(candidate.first)
                            index(candidate.first, candidate.second)
                        } catch (ex: Exception) {
                            logger.error("Unable to index linking entity with linking_id ${candidate.first}.", ex)
                        } finally {
                            unLock(candidate.first)
                        }
                    }
                }
    }

    @Suppress("UNUSED")
    @Timed
    @Scheduled(fixedRate = LINKING_INDEX_RATE)
    fun updateCandidateList() {
        if (indexerConfiguration.backgroundLinkingIndexingEnabled) {
            executor.submit {
                logger.info("Registering linking ids needing indexing.")
                var dirtyLinkingIds = getDirtyLinkingIds()
                while (dirtyLinkingIds.iterator().hasNext()) {
                    dirtyLinkingIds.forEach(candidates::put)
                    dirtyLinkingIds = getDirtyLinkingIds()
                }
            }
        }
    }

    private fun index(linkingId: UUID, lastWrite: OffsetDateTime) {
        logger.info("Starting background linking indexing task for linking id $linkingId.")
        val watch = Stopwatch.createStarted()

        // get data for linking id by entity set ids and property ids
        val dirtyLinkingIdsByEntitySetId = getEntitySetIdsOfLinkingId(linkingId) // entity_set_id/linking_id
                .toMap()

        val propertyTypesOfEntitySets = dirtyLinkingIdsByEntitySetId // entity_set_id/property_type_id/property_type
                .map { it.key to personPropertyTypes }
                .toMap()

        // linking_id/(normal)entity_set_id/property_type_id
        val linkedEntityData = dataStore.getLinkedEntityDataByLinkingIdWithMetadata(
                        dirtyLinkingIdsByEntitySetId,
                        propertyTypesOfEntitySets,
                        EnumSet.of(MetadataOption.LAST_WRITE))

        val indexCount = indexLinkedEntity( linkingId, lastWrite, personEntityType.id, linkedEntityData )

        logger.info(
                "Finished linked indexing {} elements with linking id {} in {} ms",
                indexCount,
                linkingId,
                watch.elapsed(TimeUnit.MILLISECONDS)
        )
    }

    private fun indexLinkedEntity(
            linkingId: UUID,
            lastWrite: OffsetDateTime,
            entityTypeId: UUID,
            dataByLinkingIdByEntitySetId: Map<UUID, Map<UUID, Map<UUID, Set<Any>>>> //linkingId -> es -> ekid -> dataz
    ): Int {
        if ( elasticsearchApi.createBulkLinkedData( entityTypeId, dataByLinkingIdByEntitySetId )) {
            return 0
        }
        return dataManager.markAsIndexed(
                dataByLinkingIdByEntitySetId.keys.map { it to mapOf(linkingId to lastWrite) }.toMap(),
                true
        )
    }

    private fun lock(linkingId: UUID) {
        val existingExpiration = linkingIndexingLocks.putIfAbsent(
                linkingId,
                Instant.now().plusMillis(LINKING_INDEXING_TIMEOUT_MILLIS).toEpochMilli()
        )
        check(existingExpiration == null) { "Unable to lock $linkingId. Existing lock expires at $existingExpiration." }
    }

    private fun unLock(linkingId: UUID) {
        linkingIndexingLocks.delete(linkingId)
    }

    /**
     * Returns the linking ids, which are needing to be indexed.
     */
    private fun getDirtyLinkingIds(): PostgresIterable<Pair<UUID, OffsetDateTime>> {
        return PostgresIterable(Supplier<StatementHolder> {
            val connection = hds.connection
            val stmt = connection.prepareStatement(selectDirtyLinkingIds())
            val rs = stmt.executeQuery()
            StatementHolder(connection, stmt, rs)
        }, Function<ResultSet, Pair<UUID, OffsetDateTime>> {
            ResultSetAdapters.linkingId(it) to ResultSetAdapters.lastWriteTyped(it)
        })
    }

    private fun selectDirtyLinkingIds(): String {
        return "SELECT ${LINKING_ID.name}, ${DataTables.LAST_WRITE.name} " +
                "FROM ${IDS.name} " +
                "WHERE ${LINKING_ID.name} IS NOT NULL " +
                    "AND ${LAST_INDEX.name} >= ${LAST_WRITE.name} " +
                    "AND ${LAST_LINK.name} >= ${LAST_WRITE.name} " +
                    "AND ${LAST_LINK_INDEX.name} < ${LAST_WRITE.name} " +
                    "AND ${VERSION.name} > 0 " +
                    "AND ${LINKING_ID.name} IS NOT NULL " +
                "LIMIT $FETCH_SIZE"
    }

    private fun getEntitySetIdsOfLinkingId(linkingId: UUID): PostgresIterable<Pair<UUID, Optional<Set<UUID>>>> {
        return PostgresIterable(Supplier<StatementHolder> {
            val connection = hds.connection
            val stmt = connection.prepareStatement(selectLinkingIdsByEntitySetIds())
            stmt.setObject(1, linkingId)
            val rs = stmt.executeQuery()
            StatementHolder(connection, stmt, rs)
        }, Function<ResultSet, Pair<UUID, Optional<Set<UUID>>>> {
            ResultSetAdapters.entitySetId(it) to Optional.of(ResultSetAdapters.linkingIds(it))
        })
    }

    private fun selectLinkingIdsByEntitySetIds(): String {
        return "SELECT ${ENTITY_SET_ID.name}, array_agg(${LINKING_ID.name}) AS ${LINKING_ID.name} " +
                "FROM ${IDS.name} " +
                "WHERE ${LINKING_ID.name} = ? " +
                "GROUP BY ${ENTITY_SET_ID.name}"
    }
}