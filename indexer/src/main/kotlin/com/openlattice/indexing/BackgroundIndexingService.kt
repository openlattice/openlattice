/*
 * Copyright (C) 2018. OpenLattice, Inc.
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
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.core.IMap
import com.hazelcast.query.Predicate
import com.hazelcast.query.Predicates
import com.hazelcast.query.QueryConstants
import com.openlattice.conductor.rpc.ConductorElasticsearchApi
import com.openlattice.data.storage.IndexingMetadataManager
import com.openlattice.data.storage.PostgresEntityDataQueryService
import com.openlattice.edm.EntitySet
import com.openlattice.edm.type.EntityType
import com.openlattice.edm.type.PropertyType
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.indexing.configuration.IndexerConfiguration
import com.openlattice.postgres.DataTables.LAST_INDEX
import com.openlattice.postgres.DataTables.LAST_WRITE
import com.openlattice.postgres.PostgresArrays
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresTable.IDS
import com.openlattice.postgres.ResultSetAdapters
import com.openlattice.postgres.streams.BasePostgresIterable
import com.openlattice.postgres.streams.PreparedStatementHolderSupplier
import com.zaxxer.hikari.HikariDataSource
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import java.time.OffsetDateTime
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock

const val EXPIRATION_MILLIS = 60_000L
const val INDEX_RATE = 300_000L
const val FETCH_SIZE = 128_000
const val INDEX_SIZE = 32_000

class BackgroundIndexingService(
        hazelcastInstance: HazelcastInstance,
        private val indexerConfiguration: IndexerConfiguration,
        private val hds: HikariDataSource,
        private val dataQueryService: PostgresEntityDataQueryService,
        private val elasticsearchApi: ConductorElasticsearchApi,
        private val dataManager: IndexingMetadataManager
) {
    companion object {
        private val logger = LoggerFactory.getLogger(BackgroundIndexingService::class.java)!!
    }

    private val propertyTypes: IMap<UUID, PropertyType> = hazelcastInstance.getMap(HazelcastMap.PROPERTY_TYPES.name)
    private val entityTypes: IMap<UUID, EntityType> = hazelcastInstance.getMap(HazelcastMap.ENTITY_TYPES.name)
    private val entitySets: IMap<UUID, EntitySet> = hazelcastInstance.getMap(HazelcastMap.ENTITY_SETS.name)

    private val indexingLocks: IMap<UUID, Long> = hazelcastInstance.getMap(HazelcastMap.INDEXING_LOCKS.name)

    init {
        indexingLocks.addIndex(QueryConstants.THIS_ATTRIBUTE_NAME.value(), true)
    }

    private val taskLock = ReentrantLock()

    @Suppress("UNCHECKED_CAST", "UNUSED")
    @Scheduled(fixedRate = EXPIRATION_MILLIS)
    fun scavengeIndexingLocks() {
        indexingLocks.removeAll(
                Predicates.lessThan(
                        QueryConstants.THIS_ATTRIBUTE_NAME.value(),
                        System.currentTimeMillis()
                ) as Predicate<UUID, Long>
        )
    }

    @Suppress("UNUSED")
    @Scheduled(fixedRate = INDEX_RATE)
    fun indexUpdatedEntitySets() {
        logger.info("Starting background indexing task.")
        //Keep number of indexing jobs under control
        if (!taskLock.tryLock()) {
            logger.info("Not starting new indexing job as an existing one is running.")
            return
        }
        try {
            ensureAllEntityTypeIndicesExist()
            if (!indexerConfiguration.backgroundIndexingEnabled) {
                logger.info("Skipping background indexing as it is not enabled.")
                return
            }
            val w = Stopwatch.createStarted()
            //We shuffle entity sets to make sure we have a chance to work share and index everything
            val lockedEntitySets = entitySets.values
                    .shuffled()
                    .filter { tryLockEntitySet(it) }
                    .filter { it.name != "OpenLattice Audit Entity Set" } //TODO: Clean out audit entity set from prod

            val totalIndexed = lockedEntitySets
                    .parallelStream()
                    .filter { !it.isLinking }
                    .mapToInt { indexEntitySet(it) }
                    .sum()

            lockedEntitySets.forEach(this::deleteIndexingLock)

            logger.info(
                    "Completed indexing {} elements in {} ms",
                    totalIndexed,
                    w.elapsed(TimeUnit.MILLISECONDS)
            )
        } finally {
            taskLock.unlock()
        }
    }

    private fun ensureAllEntityTypeIndicesExist() {
        val existingIndices = elasticsearchApi.entityTypesWithIndices
        val missingIndices = entityTypes.keys - existingIndices
        if (missingIndices.isNotEmpty()) {
            val missingEntityTypes = entityTypes.getAll(missingIndices)
            logger.info("The following entity types were missing indices: {}", missingEntityTypes.keys)
            missingEntityTypes.values.forEach { et ->
                val missingEntityTypePropertyTypes = propertyTypes.getAll(et.properties)
                elasticsearchApi.saveEntityTypeToElasticsearch(
                        et,
                        missingEntityTypePropertyTypes.values.toList()
                )
                logger.info("Created missing index for entity type ${et.type} with id ${et.id}")
            }
        }
    }

    /**
     * Preparable sql statement to select entity key ids (with last write) of an entity set for indexing.
     * Bind order is the following:
     * 1. entity set id
     * 2. partition (array)
     * 3. partition version
     */
    private fun getEntityDataKeysQuery(reindexAll: Boolean = false, getTombstoned: Boolean = false): String {
        val dirtyIdsClause = if (!reindexAll) {
            "${LAST_INDEX.name} < ${LAST_WRITE.name}"
        } else {
            ""
        }
        val versionsClause = "${VERSION.name} ${if (getTombstoned) "<=" else ">"} 0"

        return "SELECT ${ID.name}, ${LAST_WRITE.name} FROM ${IDS.name} " +
                "WHERE ${ENTITY_SET_ID.name} = ? " +
                "AND ${PARTITION.name} = ANY(?) " +
                "AND ${PARTITIONS_VERSION.name} = ? " +
                "AND $versionsClause " +
                "AND $dirtyIdsClause "
    }

    private fun getEntityDataKeys(
            entitySet: EntitySet,
            reindexAll: Boolean = false,
            getTombstoned: Boolean = false
    ): BasePostgresIterable<Pair<UUID, OffsetDateTime>> {
        return BasePostgresIterable(
                PreparedStatementHolderSupplier(hds, getEntityDataKeysQuery(reindexAll, getTombstoned), FETCH_SIZE) {
                    it.setObject(1, entitySet.id)
                    it.setArray(2, PostgresArrays.createIntArray(it.connection, entitySet.partitions))
                    it.setInt(3, entitySet.partitionsVersion)
                }
        ) { ResultSetAdapters.id(it) to ResultSetAdapters.lastWriteTyped(it) }
    }

    private fun getPropertyTypeForEntityType(entityTypeId: UUID): Map<UUID, PropertyType> {
        return propertyTypes
                .getAll(entityTypes[entityTypeId]?.properties ?: setOf())
                .filter { it.value.datatype != EdmPrimitiveTypeKind.Binary }
    }

    /**
     * Creates missing documents for non-indexed entities in entity set and deletes documents of cleared/deleted
     * entities in entity set.
     *
     * @param entitySet The entity set that is about to get indexed.
     * @param reindexAll Indicator whether it should re-index all entities found in the entity set or just the ones,
     * which are not yet indexed.
     */
    private fun indexEntitySet(entitySet: EntitySet, reindexAll: Boolean = false): Int {

        val upsertedEntityCount = indexEntitiesInEntitySet(entitySet, reindexAll, indexTombstoned = false)
        val tombstonedEntityCount = indexEntitiesInEntitySet(entitySet, reindexAll, indexTombstoned = true)

        return upsertedEntityCount + tombstonedEntityCount
    }

    /**
     * @param entitySet The entity set that is about to get indexed.
     * @param reindexAll Indicator whether it should re-index all entities found in the entity set or just the ones,
     * which are not yet indexed.
     *
     * NOTE: If it is set to true, it also won't mark entities as indexed.
     * @param indexTombstoned Indicator whether it should un-index cleared ([VERSION] < 0) and deleted ([VERSION] = 0)
     * entities or index still active ones ([VERSION] > 0).
     */
    private fun indexEntitiesInEntitySet(
            entitySet: EntitySet,
            reindexAll: Boolean = false,
            indexTombstoned: Boolean = false
    ): Int {
        logger.info(
                "Starting indexing for entity set {} with id {}",
                entitySet.name,
                entitySet.id
        )

        val esw = Stopwatch.createStarted()
        val entityKeyIdsWithLastWrite = getEntityDataKeys(entitySet, reindexAll, indexTombstoned)

        val propertyTypes = getPropertyTypeForEntityType(entitySet.entityTypeId)

        var indexCount = 0
        var entityKeyIdsIterator = entityKeyIdsWithLastWrite.iterator()

        while (entityKeyIdsIterator.hasNext()) {
            updateExpiration(entitySet)
            while (entityKeyIdsIterator.hasNext()) {
                val batch = getBatch(entityKeyIdsIterator)
                indexCount += if (indexTombstoned) {
                    unindexEntities(entitySet, batch, !reindexAll)
                } else {
                    indexEntities(entitySet, batch, propertyTypes, !reindexAll)
                }
            }
            entityKeyIdsIterator = entityKeyIdsWithLastWrite.iterator()
        }

        logger.info(
                "Finished indexing {} elements from entity set {} in {} ms",
                indexCount,
                entitySet.name,
                esw.elapsed(TimeUnit.MILLISECONDS)
        )

        return indexCount
    }

    internal fun indexEntities(
            entitySet: EntitySet,
            batchToIndex: Map<UUID, OffsetDateTime>,
            propertyTypeMap: Map<UUID, PropertyType>,
            markAsIndexed: Boolean = true
    ): Int {
        val esb = Stopwatch.createStarted()
        val entitiesById = dataQueryService.getEntitiesWithPropertyTypeIds(
                mapOf(entitySet.id to Optional.of(batchToIndex.keys)),
                mapOf(entitySet.id to propertyTypeMap)).toMap()

        logger.info("Loading data for indexEntities took {} ms", esb.elapsed(TimeUnit.MILLISECONDS))

        if (entitiesById.size != batchToIndex.size) {
            logger.error(
                    "Expected {} items to index but received {}. Marking as indexed to prevent infinite loop.",
                    batchToIndex.size,
                    entitiesById.size
            )
        }

        if (entitiesById.isEmpty()
                || !elasticsearchApi.createBulkEntityData(entitySet.entityTypeId, entitySet.id, entitiesById)) {
            logger.error("Failed to index elements with entitiesById: {}", entitiesById)
            return 0
        }

        val indexCount = if (markAsIndexed) {
            dataManager.markAsIndexed(mapOf(entitySet.id to batchToIndex), false)
        } else {
            batchToIndex.size
        }

        logger.info(
                "Indexed batch of {} elements for {} ({}) in {} ms",
                indexCount,
                entitySet.name,
                entitySet.id,
                esb.elapsed(TimeUnit.MILLISECONDS)
        )

        return indexCount

    }

    private fun unindexEntities(
            entitySet: EntitySet,
            batchToIndex: Map<UUID, OffsetDateTime>,
            markAsIndexed: Boolean = true
    ): Int {
        val esb = Stopwatch.createStarted()

        val indexCount: Int

        if (batchToIndex.isNotEmpty()
                && elasticsearchApi.deleteEntityDataBulk(entitySet.entityTypeId, batchToIndex.keys)) {
            indexCount = if (markAsIndexed) {
                dataManager.markAsIndexed(mapOf(entitySet.id to batchToIndex), false)
            } else {
                batchToIndex.size
            }

            logger.info(
                    "Un-indexed batch of {} elements for {} ({}) in {} ms",
                    indexCount,
                    entitySet.name,
                    entitySet.id,
                    esb.elapsed(TimeUnit.MILLISECONDS)
            )
        } else {
            indexCount = 0
            logger.error(
                    "Failed to un-index elements of entity set {} with entity key ids: {}",
                    entitySet.id,
                    batchToIndex.keys
            )

        }
        return indexCount
    }

    private fun tryLockEntitySet(entitySet: EntitySet): Boolean {
        return indexingLocks.putIfAbsent(entitySet.id, System.currentTimeMillis() + EXPIRATION_MILLIS) == null
    }

    private fun deleteIndexingLock(entitySet: EntitySet) {
        indexingLocks.delete(entitySet.id)
    }

    private fun updateExpiration(entitySet: EntitySet) {
        indexingLocks.set(entitySet.id, System.currentTimeMillis() + EXPIRATION_MILLIS)
    }

    private fun getBatch(entityKeyIdStream: Iterator<Pair<UUID, OffsetDateTime>>): Map<UUID, OffsetDateTime> {
        val entityKeyIds = HashMap<UUID, OffsetDateTime>(INDEX_SIZE)

        var i = 0
        while (entityKeyIdStream.hasNext() && i < INDEX_SIZE) {
            val entityWithLastWrite = entityKeyIdStream.next()
            entityKeyIds[entityWithLastWrite.first] = entityWithLastWrite.second
            ++i
        }

        return entityKeyIds
    }
}