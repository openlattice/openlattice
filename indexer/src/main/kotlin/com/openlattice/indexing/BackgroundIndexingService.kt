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
import com.openlattice.data.storage.PostgresEntityDataQueryService
import com.openlattice.edm.EntitySet
import com.openlattice.edm.type.EntityType
import com.openlattice.edm.type.PropertyType
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.neuron.audit.AuditEntitySetUtils
import com.openlattice.postgres.DataTables
import com.openlattice.postgres.DataTables.*
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
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantLock
import java.util.function.Function
import java.util.function.Supplier

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */

private const val EXPIRATION_MILLIS = 60000L
private const val INDEX_RATE = 1000L

class BackgroundIndexingService(
        private val hds: HikariDataSource,
        hazelcastInstance: HazelcastInstance,
        private val dataQueryService: PostgresEntityDataQueryService,
        private val elasticsearchApi: ConductorElasticsearchApi
) {
    companion object {
        private val logger = LoggerFactory.getLogger(BackgroundIndexingService::class.java)!!
        const val INDEX_SIZE = 32000
        const val FETCH_SIZE = 128000
    }

    private val propertyTypes: IMap<UUID, PropertyType> = hazelcastInstance.getMap(HazelcastMap.PROPERTY_TYPES.name)
    private val entityTypes: IMap<UUID, EntityType> = hazelcastInstance.getMap(HazelcastMap.ENTITY_TYPES.name)
    private val entitySets: IMap<UUID, EntitySet> = hazelcastInstance.getMap(HazelcastMap.ENTITY_SETS.name)
    private val indexingLocks: IMap<UUID, Long> = hazelcastInstance.getMap(HazelcastMap.INDEXING_LOCKS.name)

    init {
        indexingLocks.addIndex(QueryConstants.THIS_ATTRIBUTE_NAME.value(), true)
    }

    private var taskLock = ReentrantLock()

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
                val totalIndexed = AtomicInteger()
                val w = Stopwatch.createStarted()
                entitySets.values.parallelStream()
                        .filter { it.name != AuditEntitySetUtils.AUDIT_ENTITY_SET_NAME }
                        .mapToInt(this::indexEntitySet)
                        .peek {
                            logger.info(
                                    "Processed {} elements in {} ms",
                                    totalIndexed.addAndGet(it),
                                    w.elapsed(TimeUnit.MILLISECONDS)
                            )
                        }
                        .sum()
                logger.info(
                        "Completed indexing {} elements in {} ms",
                        totalIndexed.get(),
                        w.elapsed(TimeUnit.MILLISECONDS)
                )
            } finally {
                taskLock.unlock()
            }
        } else {
            logger.info("Skipping indexing as thread limit hit.")
        }
    }

    private fun getDirtyEntitiesQuery(entitySetId: UUID): String {
        return "SELECT * FROM ${quote(DataTables.entityTableName(entitySetId))} " +
                "WHERE ${LAST_INDEX.name} < ${LAST_WRITE.name} LIMIT $FETCH_SIZE"
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

    private fun getPropertyTypesByEntityTypesById(): Map<UUID, Map<UUID, PropertyType>> {
        return entityTypes.entries.map {
            it.key to propertyTypes
                    .getAll(it.value.properties)
                    .filter { it.value.datatype != EdmPrimitiveTypeKind.Binary }
                    .toMap()
        }.toMap()
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
            updateExpiration(entitySet.id)
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
        val entitySetId = entitySet.id
        var indexCount = 0
        updateExpiration(entitySetId)
        val entitiesById = dataQueryService.getEntitiesById(
                entitySetId,
                propertyTypeMap,
                batchToIndex
        )

        if (elasticsearchApi.createBulkEntityData(entitySetId, entitiesById)) {
            indexCount += dataQueryService.markAsIndexed(entitySetId, batchToIndex)
            logger.info(
                    "Indexed batch of {} elements for {} ({}) in {} ms",
                    indexCount,
                    entitySet.name,
                    entitySet.id,
                    esb.elapsed(TimeUnit.MILLISECONDS)
            )
            updateExpiration(entitySet.id)
        }

        return indexCount
    }

    private fun updateExpiration(entitySetId: UUID) {
        indexingLocks.set(entitySetId, System.currentTimeMillis() + EXPIRATION_MILLIS)
    }

    private fun getBatch(entityKeyIdStream: Iterator<UUID>): MutableSet<UUID> {
        val entityKeyIds = HashSet<UUID>(INDEX_SIZE)

        var i = 0
        while (i < INDEX_SIZE && entityKeyIdStream.hasNext()) {
            entityKeyIds.add(entityKeyIdStream.next())
            ++i
        }
        return entityKeyIds
    }
}