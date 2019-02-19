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
import com.openlattice.postgres.DataTables.LAST_INDEX
import com.openlattice.postgres.DataTables.LAST_WRITE
import com.openlattice.postgres.PostgresColumn.ENTITY_SET_ID
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

/**
 *
 */

const val EXPIRATION_MILLIS = 60000L
const val INDEX_RATE = 30000L
const val FETCH_SIZE = 128000

class BackgroundIndexingService(
        private val hds: HikariDataSource,
        hazelcastInstance: HazelcastInstance,
        private val dataQueryService: PostgresEntityDataQueryService,
        private val elasticsearchApi: ConductorElasticsearchApi,
        private val dataManager: IndexingMetadataManager
) {
    companion object {
        private val logger = LoggerFactory.getLogger(BackgroundIndexingService::class.java)!!
        const val INDEX_SIZE = 32000
    }

    private val propertyTypes: IMap<UUID, PropertyType> = hazelcastInstance.getMap(HazelcastMap.PROPERTY_TYPES.name)
    private val entityTypes: IMap<UUID, EntityType> = hazelcastInstance.getMap(HazelcastMap.ENTITY_TYPES.name)
    private val entitySets: IMap<UUID, EntitySet> = hazelcastInstance.getMap(HazelcastMap.ENTITY_SETS.name)
    private val indexingLocks: IMap<UUID, Long> = hazelcastInstance.getMap(HazelcastMap.INDEXING_LOCKS.name)

    init {
        indexingLocks.addIndex(QueryConstants.THIS_ATTRIBUTE_NAME.value(), true)
    }

    private val taskLock = ReentrantLock()

    @Scheduled(fixedRate = EXPIRATION_MILLIS)
    fun scavengeIndexingLocks() {
        indexingLocks.removeAll(
                Predicates.lessThan(
                        QueryConstants.THIS_ATTRIBUTE_NAME.value(),
                        System.currentTimeMillis()
                ) as Predicate<UUID, Long>
        )
    }

    @Scheduled(fixedRate = INDEX_RATE)
    fun indexUpdatedEntitySets() {
        logger.info("Starting background indexing task.")
        //Keep number of indexing jobs under control
        if (taskLock.tryLock()) {
            try {
                ensureAllEntitySetIndexesExist()
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
        } else {
            logger.info("Not starting new indexing job as an existing one is running.")
        }
    }

    private fun ensureAllEntitySetIndexesExist() {
        val existingIndices = elasticsearchApi.entitySetWithIndices
        val missingIndices = entitySets.keys - existingIndices
        if (missingIndices.isNotEmpty()) {
            val missingEntitySets = entitySets.getAll(missingIndices)
            logger.info("The following entity sets where missing indices: {}", missingEntitySets)
            missingEntitySets.values.forEach { es ->
                val missingEntitySetPropertyTypes = propertyTypes.getAll(entityTypes[es.entityTypeId]!!.properties)
                elasticsearchApi.saveEntitySetToElasticsearch(
                        es,
                        missingEntitySetPropertyTypes.values.toList())
                logger.info("Created missing index for entity set ${es.name} with id ${es.entityTypeId}")
            }
        }
    }

    private fun getDirtyEntitiesQuery(entitySetId: UUID): String {
        return "SELECT * FROM ${IDS.name} " +
                "WHERE ${ENTITY_SET_ID.name} = '$entitySetId' AND ${LAST_INDEX.name} < ${LAST_WRITE.name} LIMIT $FETCH_SIZE"
    }

    private fun getDirtyEntityKeyIds(entitySetId: UUID): PostgresIterable<UUID> {
        return PostgresIterable(Supplier<StatementHolder> {
            val connection = hds.connection
            val stmt = connection.createStatement()
            val rs = stmt.executeQuery(getDirtyEntitiesQuery(entitySetId))
            StatementHolder(connection, stmt, rs)
        }, Function<ResultSet, UUID> { ResultSetAdapters.id(it) })
    }

    private fun getPropertyTypeForEntityType(entityTypeId: UUID): Map<UUID, PropertyType> {
        return propertyTypes
                .getAll(entityTypes[entityTypeId]?.properties ?: setOf())
                .filter { it.value.datatype != EdmPrimitiveTypeKind.Binary }
    }

    private fun indexEntitySet(entitySet: EntitySet): Int {
        logger.info(
                "Starting indexing for entity set {} with id {}",
                entitySet.name,
                entitySet.id
        )

        val esw = Stopwatch.createStarted()
        val entityKeyIds = getDirtyEntityKeyIds(entitySet.id)
        val propertyTypes = getPropertyTypeForEntityType(entitySet.entityTypeId)

        var indexCount = 0
        var entityKeyIdsIterator = entityKeyIds.iterator()

        while (entityKeyIdsIterator.hasNext()) {
            updateExpiration(entitySet)
            while (entityKeyIdsIterator.hasNext()) {
                val batch = getBatch(entityKeyIdsIterator)
                indexCount += indexEntities(entitySet, batch, propertyTypes)
            }
            entityKeyIdsIterator = entityKeyIds.iterator()
        }

        logger.info(
                "Finished indexing {} elements from entity set {} in {} ms",
                indexCount,
                entitySet.name,
                esw.elapsed(TimeUnit.MILLISECONDS)
        )

        return indexCount
    }

    private fun indexEntities(
            entitySet: EntitySet,
            batchToIndex: Set<UUID>,
            propertyTypeMap: Map<UUID, PropertyType>
    ): Int {
        val esb = Stopwatch.createStarted()
<<<<<<< HEAD

        val entitiesById = dataQueryService.getEntitiesById(entitySet.id, propertyTypeMap, batchToIndex)

        if( entitiesById.size != batchToIndex.size ) {
            logger.error("Expected {} items to index but received {}. Marking as indexed to prevent infinite loop.", batchToIndex.size,  entitiesById.size )
        }
        return if (batchToIndex.isNotEmpty() && elasticsearchApi.createBulkEntityData(entitySet.id, entitiesById)) {
            val indexCount = dataManager.markAsIndexed(mapOf(entitySet.id to Optional.of(batchToIndex)), false)
=======
        val indexCount : Int
        val entitiesById = dataQueryService.getEntitiesById(entitySet.id, propertyTypeMap, batchToIndex)

        if (entitiesById.isNotEmpty() && elasticsearchApi.createBulkEntityData(entitySet.id, entitiesById)) {
            indexCount = dataManager.markAsIndexed(mapOf(entitySet.id to Optional.of(batchToIndex)), false)
>>>>>>> develop
            logger.info(
                    "Indexed batch of {} elements for {} ({}) in {} ms",
                    indexCount,
                    entitySet.name,
                    entitySet.id,
                    esb.elapsed(TimeUnit.MILLISECONDS)
            )
<<<<<<< HEAD
            indexCount
        } else  {
            0
=======
        } else {
            indexCount = 0
            logger.error("Failed to index elements with entitiesById: {}", entitiesById)
>>>>>>> develop
        }

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

    private fun getBatch(entityKeyIdStream: Iterator<UUID>): Set<UUID> {
        val entityKeyIds = HashSet<UUID>(INDEX_SIZE)

        var i = 0
        while (entityKeyIdStream.hasNext() && i < INDEX_SIZE) {
            entityKeyIds.add(entityKeyIdStream.next())
            ++i
        }

        return entityKeyIds
    }
}