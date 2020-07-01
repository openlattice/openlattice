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

package com.openlattice.graph

import com.codahale.metrics.MetricRegistry
import com.codahale.metrics.annotation.Timed
import com.geekbeast.metrics.time
import com.google.common.annotations.VisibleForTesting
import com.google.common.collect.ImmutableList
import com.google.common.collect.Multimaps
import com.google.common.collect.SetMultimap
import com.openlattice.analysis.*
import com.openlattice.analysis.requests.*
import com.openlattice.data.*
import com.openlattice.data.storage.PostgresEntityDataQueryService
import com.openlattice.data.storage.entityKeyIdColumns
import com.openlattice.data.storage.partitions.getPartition
import com.openlattice.data.storage.partitions.PartitionManager
import com.openlattice.data.storage.selectEntitySetWithCurrentVersionOfPropertyTypes
import com.openlattice.datastore.services.EntitySetManager
import com.openlattice.edm.type.PropertyType
import com.openlattice.graph.core.GraphService
import com.openlattice.graph.core.NeighborSets
import com.openlattice.graph.edge.Edge
import com.openlattice.postgres.DataTables.quote
import com.openlattice.postgres.PostgresArrays
import com.openlattice.postgres.PostgresColumn
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresTable.*
import com.openlattice.postgres.ResultSetAdapters
import com.openlattice.postgres.streams.BasePostgresIterable
import com.openlattice.postgres.streams.PostgresIterable
import com.openlattice.postgres.streams.StatementHolder
import com.openlattice.search.requests.EntityNeighborsFilter
import com.zaxxer.hikari.HikariDataSource
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.security.InvalidParameterException
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.time.LocalDate
import java.time.LocalTime
import java.time.OffsetDateTime
import java.util.*
import java.util.function.Function
import java.util.function.Supplier
import java.util.stream.Stream
import kotlin.streams.asSequence
import kotlin.streams.toList

/**
 *
 */

const val SELF_ENTITY_SET_ID = "self_entity_set_id"
const val SELF_ENTITY_KEY_ID = "self_entity_key_id"
private const val BATCH_SIZE = 10_000

private val logger = LoggerFactory.getLogger(Graph::class.java)

@Service
class Graph(
        private val hds: HikariDataSource,
        private val reader: HikariDataSource,
        private val entitySetManager: EntitySetManager,
        private val partitionManager: PartitionManager,
        private val pgDataQueryService: PostgresEntityDataQueryService,
        private val entityKeyIdService: EntityKeyIdService,
        private val metricRegistry: MetricRegistry
) : GraphService {

    /* Create */

    override fun createEdges(keys: MutableSet<DataEdgeKey>): WriteEvent {
        val partitionsInfoByEntitySet = partitionManager.getPartitionsByEntitySetId(
                keys.flatMap { listOf(it.src, it.dst, it.edge) }
                        .map { it.entitySetId }.toSet()
        )
                .mapValues { it.value.toList() }

        hds.connection.use { connection ->
            val ps = connection.prepareStatement(EDGES_UPSERT_SQL)
            val version = System.currentTimeMillis()
            val versions = PostgresArrays.createLongArray(connection, ImmutableList.of(version))

            ps.use {
                keys.forEach { dataEdgeKey ->
                    bindColumnsForEdge(ps, dataEdgeKey, version, versions, partitionsInfoByEntitySet)
                }
                return WriteEvent(version, ps.executeBatch().sum())
            }
        }
    }


    private fun addKeyIds(ps: PreparedStatement, dataEdgeKey: DataEdgeKey, startIndex: Int = 1) {
        val edk = dataEdgeKey.src
        val partitions = partitionManager.getEntitySetPartitions(edk.entitySetId)
        val partition = getPartition(
                edk.entityKeyId, partitions.toList()
        )
        ps.setObject(startIndex, partition)
        ps.setObject(startIndex + 1, dataEdgeKey.src.entityKeyId)
        ps.setObject(startIndex + 2, dataEdgeKey.dst.entityKeyId)
        ps.setObject(startIndex + 3, dataEdgeKey.edge.entityKeyId)
        ps.addBatch()
    }

    /* Delete  */

    override fun clearEdges(keys: Iterable<DataEdgeKey>): Int {
        val version = -System.currentTimeMillis()
        return lockAndOperateOnEdges(keys, CLEAR_BY_VERTEX_SQL) { lockStmt, operationStmt, dataEdgeKey ->

            addKeyIds(lockStmt, dataEdgeKey)

            clearEdgesAddVersion(operationStmt, version)
            addKeyIds(operationStmt, dataEdgeKey, 3)
            clearEdgesAddVersion(operationStmt, version)
            addKeyIds(operationStmt, dataEdgeKey, 3)
            clearEdgesAddVersion(operationStmt, version)
            addKeyIds(operationStmt, dataEdgeKey, 3)
        }
    }

    private fun lockAndOperateOnEdges(
            keys: Iterable<DataEdgeKey>,
            statement: String,
            statementSupplier: (lockStmt: PreparedStatement, operationStmt: PreparedStatement, dataEdgeKey: DataEdgeKey) -> Unit
    ): Int {
        hds.connection.use { connection ->
            var updates = 0
            connection.autoCommit = false
            connection.prepareStatement(LOCK_BY_VERTEX_SQL).use { psLocks ->
                connection.prepareStatement(statement).use { psExecute ->
                    keys.forEach { dataEdgeKey ->
                        statementSupplier(psLocks, psExecute, dataEdgeKey)
                    }
                    psLocks.executeBatch()
                    updates = psExecute.executeBatch().sum()
                }
            }
            connection.commit()

            connection.autoCommit = true
            return updates
        }
    }

    private fun clearEdgesAddVersion(ps: PreparedStatement, version: Long) {
        ps.setLong(1, version)
        ps.setLong(2, version)
    }

    override fun deleteEdges(keys: Iterable<DataEdgeKey>): WriteEvent {
        val updates = lockAndOperateOnEdges(keys, DELETE_BY_VERTEX_SQL) { lockStmt, operationStmt, dataEdgeKey ->
            addKeyIds(lockStmt, dataEdgeKey)
            addKeyIds(operationStmt, dataEdgeKey)
        }
        return WriteEvent(System.currentTimeMillis(), updates)
    }

    /* Select */

    override fun getEdgeKeysContainingEntities(
            entitySetId: UUID, entityKeyIds: Set<UUID>, includeClearedEdges: Boolean
    ): PostgresIterable<DataEdgeKey> {
        val sql = if (includeClearedEdges) BULK_NEIGHBORHOOD_SQL else BULK_NON_TOMBSTONED_NEIGHBORHOOD_SQL
        return PostgresIterable(
                Supplier {
                    val connection = hds.connection
                    connection.autoCommit = false
                    val idArr = PostgresArrays.createUuidArray(connection, entityKeyIds)
                    val stmt = connection.prepareStatement(sql)
                    stmt.setArray(1, idArr)
                    stmt.setObject(2, entitySetId)
                    stmt.setArray(3, idArr)
                    stmt.setObject(4, entitySetId)
                    stmt.setArray(5, idArr)
                    stmt.setObject(6, entitySetId)
                    stmt.fetchSize = BATCH_SIZE
                    val rs = stmt.executeQuery()
                    StatementHolder(connection, stmt, rs)
                },
                Function<ResultSet, DataEdgeKey> { ResultSetAdapters.edgeKey(it) }
        )
    }

    override fun getEdgeKeysOfEntitySet(
            entitySetId: UUID, includeClearedEdges: Boolean
    ): PostgresIterable<DataEdgeKey> {
        val sql = if (includeClearedEdges) NEIGHBORHOOD_OF_ENTITY_SET_SQL else NON_TOMBSTONED_NEIGHBORHOOD_OF_ENTITY_SET_SQL
        return PostgresIterable(
                Supplier {
                    val connection = hds.connection
                    connection.autoCommit = false
                    val stmt = connection.prepareStatement(sql)
                    stmt.setObject(1, entitySetId)
                    stmt.setObject(2, entitySetId)
                    stmt.setObject(3, entitySetId)
                    stmt.fetchSize = BATCH_SIZE
                    val rs = stmt.executeQuery()
                    StatementHolder(connection, stmt, rs)
                },
                Function<ResultSet, DataEdgeKey> { ResultSetAdapters.edgeKey(it) }
        )
    }

    override fun getEdgesAndNeighborsForVertex(entitySetId: UUID, vertexId: UUID): Stream<Edge> {

        return PostgresIterable(
                Supplier {
                    val connection = hds.connection
                    val stmt = connection.prepareStatement(NEIGHBORHOOD_SQL)
                    stmt.setObject(1, vertexId)
                    stmt.setObject(2, entitySetId)
                    stmt.setObject(3, vertexId)
                    stmt.setObject(4, entitySetId)
                    stmt.setObject(5, vertexId)
                    stmt.setObject(6, entitySetId)
                    val rs = stmt.executeQuery()
                    StatementHolder(connection, stmt, rs)
                },
                Function<ResultSet, Edge> { ResultSetAdapters.edge(it) }
        ).stream()
    }

    override fun getEdgesAndNeighborsForVertices(entitySetId: UUID, filter: EntityNeighborsFilter): Stream<Edge> {
        return PostgresIterable(
                Supplier {
                    val connection = reader.connection
                    val ids = PostgresArrays.createUuidArray(connection, filter.entityKeyIds)
                    val stmt = connection.prepareStatement(getFilteredNeighborhoodSql(filter, false))
                    stmt.setArray(1, ids)
                    stmt.setObject(2, entitySetId)
                    stmt.setArray(3, ids)
                    stmt.setObject(4, entitySetId)
                    val rs = stmt.executeQuery()
                    StatementHolder(connection, stmt, rs)
                },
                Function<ResultSet, Edge> { ResultSetAdapters.edge(it) }
        ).stream()
    }


    override fun getEdgesAndNeighborsForVerticesBulk(
            entitySetIds: Set<UUID>,
            filter: EntityNeighborsFilter
    ): Stream<Edge> {
        if (entitySetIds.size == 1) {
            return getEdgesAndNeighborsForVertices(entitySetIds.first(), filter)
        }
        return PostgresIterable(
                Supplier {
                    val connection = reader.connection
                    val ids = PostgresArrays.createUuidArray(connection, filter.entityKeyIds.stream())
                    val entitySetIdsArr = PostgresArrays.createUuidArray(connection, entitySetIds.stream())
                    val stmt = connection.prepareStatement(getFilteredNeighborhoodSql(filter, true))
                    stmt.setArray(1, ids)
                    stmt.setArray(2, entitySetIdsArr)
                    stmt.setArray(3, ids)
                    stmt.setArray(4, entitySetIdsArr)
                    val rs = stmt.executeQuery()
                    StatementHolder(connection, stmt, rs)
                },
                Function<ResultSet, Edge> { ResultSetAdapters.edge(it) }
        ).stream()
    }


    /* Top utilizers */

    /**
     * 1. Compute the table of all neighbors of all relevant neighbors to person entity sets.
     * 2. Apply relevant filters for person entity sets with an inner join
     * 3. Apply relevant filters for associations with an innner join.
     *
     */
    @Timed
    override fun computeTopEntities(
            limit: Int,
            entitySetIds: Set<UUID>,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
            filteredRankings: List<AuthorizedFilteredNeighborsRanking>,
            linked: Boolean,
            linkingEntitySetId: Optional<UUID>
    ): AggregationResult {
        //Step 1:
        //Load all entity set data that satisfies filters

        //Step 2: Load all neighbors that satisfy filters

        //Step 3: Execute extractors to annotate each row

        //Step 4: Compute aggregations in memory
        val entityKeyIds = entitySetIds.associateWith { Optional.empty<Set<UUID>>() }
        val propertyTypes = authorizedPropertyTypes.values.flatMap { it.values }.associateBy { it.id }

        val srcEntities = if (linked) {
            pgDataQueryService.getLinkedEntitiesWithPropertyTypeIds(
                    entityKeyIds,
                    authorizedPropertyTypes
            ).toMap()
        } else {
            pgDataQueryService.getEntitiesWithPropertyTypeIds(
                    entityKeyIds,
                    authorizedPropertyTypes
            ).toMap()
        }

        logger.info("Source entities: {}", srcEntities.size)
        val linkedEntities = if (linked) {
            entityKeyIdService.getLinkingEntityKeyIds(entityKeyIds.keys)
        } else {
            mapOf()
        }

        val reverseLinkedEntities = linkedEntities.entries.groupBy({ it.value }, { it.key })

        val ascRankings = sortedSetOf<NeighborhoodRankingAggregationResult>()
//        val neighbors = mutableMapOf<UUID, Map<UUID, Set<Any>>>()
//        val associations = mutableMapOf<UUID, Map<UUID, Set<Any>>>()
        val edgePairs = mutableMapOf<UUID, List<Pair<UUID, UUID>>>()

        val neighborhoods = metricRegistry.time(Graph::class.java, "aggregate-neighborhood") { log, context ->
            filteredRankings.parallelStream().flatMap { authorizedFilteredNeighborsRanking ->
                logger.info("Filtered ranking: {}", authorizedFilteredNeighborsRanking)
                val assocEntitySetIds = authorizedFilteredNeighborsRanking.associationSets.keys
                val dstEntitySetIds = authorizedFilteredNeighborsRanking.entitySets.keys


                val dst = authorizedFilteredNeighborsRanking.filteredNeighborsRanking.dst

                var neighborsFilter = EntityNeighborsFilter(
                        srcEntities.keys,
                        if (dst) Optional.of(dstEntitySetIds) else Optional.of(entitySetIds),
                        if (dst) Optional.of(entitySetIds) else Optional.of(dstEntitySetIds),
                        Optional.of(assocEntitySetIds)
                )

                //Now we load all edges for this neighbor type into memory
                val edges = getEdgesAndNeighborsForVerticesBulk(entitySetIds, neighborsFilter)
                        .toList()

                logger.info("Edges: {}", edges.size)

                val associationEntityKeyIds = edges
                        .groupBy({ it.edge.entitySetId }, { it.edge.entityKeyId })
                        .mapValues { Optional.of(it.value.toSet()) }

                logger.info("Association entity key ids: {}", associationEntityKeyIds.size)

                //Purposefully verbose for clarity.
                val neighborEntityKeyIds = if (dst) {
                    edges
                            .groupBy({ it.src.entitySetId }, { it.src.entityKeyId })
                            .mapValues { Optional.of(it.value.toSet()) }
                } else {
                    edges
                            .groupBy({ it.dst.entitySetId }, { it.dst.entityKeyId })
                            .mapValues { Optional.of(it.value.toSet()) }
                }

                logger.info("Neighbor entity key ids: {}", neighborEntityKeyIds.size)

                val associationEntities = pgDataQueryService.getEntitiesWithPropertyTypeIds(
                        associationEntityKeyIds,
                        authorizedPropertyTypes,
                        authorizedFilteredNeighborsRanking.filteredNeighborsRanking.associationFilters
                ).toMap()

                val neighborEntities = pgDataQueryService.getEntitiesWithPropertyTypeIds(
                        neighborEntityKeyIds,
                        authorizedPropertyTypes,
                        authorizedFilteredNeighborsRanking.filteredNeighborsRanking.neighborFilters
                ).toMap()

                //Purposefully verbose for clarity.
                //Need to filter out associations and neighbors not matching filters.
                val groupedEdges = if (dst) {
                    edges
                            .asSequence()
                            .filter { edge ->
                                neighborEntities.containsKey(edge.src.entityKeyId) &&
                                        associationEntities.containsKey(edge.edge.entityKeyId)
                            }
                            .groupBy(
                                    {
                                        if (linked) getLinkingId(linkedEntities, it.dst.entityKeyId)
                                        else it.dst.entityKeyId
                                    },
                                    { it.edge.entityKeyId to it.src.entityKeyId }
                            )
                } else {
                    edges.asSequence()
                            .filter { edge ->
                                neighborEntities.containsKey(edge.dst.entityKeyId) &&
                                        associationEntities.containsKey(edge.edge.entityKeyId)
                            }
                            .groupBy(
                                    {
                                        if (linked) getLinkingId(linkedEntities, it.src.entityKeyId)
                                        else it.src.entityKeyId
                                    },
                                    { it.edge.entityKeyId to it.dst.entityKeyId })
                }

                logger.info("Grouped edges: {}", groupedEdges.size)
                
                edgePairs.putAll(groupedEdges)

                try {
                    computeAggregations(
                            authorizedFilteredNeighborsRanking,
                            srcEntities,
                            associationEntities,
                            neighborEntities,
                            groupedEdges,
                            propertyTypes
                    ).stream()
                } catch (e: Exception) {
                    log.error("wat", e)
                    Stream.of<FilteredNeighborsRankingAggregationResult>()
                } finally {
                    val neighborTypeId = authorizedFilteredNeighborsRanking.filteredNeighborsRanking.neighborTypeId
                    val associationTypeId = authorizedFilteredNeighborsRanking.filteredNeighborsRanking.associationTypeId
                    log.info("Aggregation for ($neighborTypeId,$associationTypeId) took ${context.stop() / 1000} ms")
                }
            }.asSequence().groupBy { it.entityKeyId }
        }

        logger.info("Neighborhoods: {}", neighborhoods.size)

        metricRegistry.time(Graph::class.java, "select-top-n") { log, context ->
            neighborhoods.forEach { (entityKeyId, results) ->

                val next = NeighborhoodRankingAggregationResult(
                        entityKeyId,
                        results.sumByDouble { it.score },
                        results
                )
                if (ascRankings.size < limit) {
                    ascRankings.add(next)
                } else {
                    if (ascRankings.first().score < next.score) {
                        ascRankings.remove(ascRankings.first())
                        ascRankings.add(next)
                    }
                }

            }
            log.info("Selecting top $limit took ${context.stop() / 1000} ms")
        }

        return metricRegistry.time(Graph::class.java, "prepare-results") { log, context ->
            val rankings = ascRankings.descendingSet()
            val ekids = if (linked) {
                rankings.flatMap { reverseLinkedEntities.getValue(it.entityKeyId) }.toSet()
            } else {
                rankings.map { it.entityKeyId }.toSet()
            }
            val allNeighborsFilter = EntityNeighborsFilter(
                    ekids,
                    Optional.of(authorizedPropertyTypes.keys),
                    Optional.of(authorizedPropertyTypes.keys),
                    Optional.of(authorizedPropertyTypes.keys)
            )
            val allNeighborEdges = getEdgesAndNeighborsForVerticesBulk(entitySetIds, allNeighborsFilter)
            val associationEntityKeyIds = mutableMapOf<UUID, Optional<MutableSet<UUID>>>()
            val neighborEntityKeyIds = mutableMapOf<UUID, Optional<MutableSet<UUID>>>()
            val edges = mutableMapOf<UUID, MutableMap<UUID, UUID>>()
            allNeighborEdges.forEach {
                associationEntityKeyIds
                        .getOrPut(it.edge.entitySetId) { Optional.of(mutableSetOf()) }
                        .get()
                        .add(it.edge.entityKeyId)

                neighborEntityKeyIds
                        .getOrPut(it.dst.entitySetId) { Optional.of(mutableSetOf()) }
                        .get()
                        .add(it.dst.entityKeyId)
                edges.getOrPut(it.src.entityKeyId) { mutableMapOf() }[it.edge.entityKeyId] = it.dst.entityKeyId
            }
            val associations = pgDataQueryService.getEntitiesWithPropertyTypeIds(
                    associationEntityKeyIds as Map<UUID, Optional<Set<UUID>>>,
                    authorizedPropertyTypes
            ).toMap()
            var neighbors = pgDataQueryService.getEntitiesWithPropertyTypeIds(
                    neighborEntityKeyIds as Map<UUID, Optional<Set<UUID>>>,
                    authorizedPropertyTypes
            ).toMap()
            val allEntities = (srcEntities + associations + neighbors).mapValues { entityPair ->
                entityPair.value.mapKeys { propertyTypes.getValue(it.key).type }
            }
            try {
                AggregationResult(
                        rankings,
                        allEntities,
                        edges
                )
            } finally {
                log.info("Preparing results took ${context.stop() / 1000} ms")
            }

        }
    }

    /**
     * Computes the aggregations for a filtered neighbor ranking
     * @param authorizedFilteredNeighborsRanking The aggregations to compute
     * @param srcEntities The source entities.
     * @param associationEntities The association entities
     * @param neighborEntities The destination entities
     * @param edges Each source entity key id and its neighborhood of entity key ids
     * @param propertyTypes Property type definitions needed to compute aggregations.
     * @return The computed aggregations.
     */
    private fun computeAggregations(
            authorizedFilteredNeighborsRanking: AuthorizedFilteredNeighborsRanking,
            srcEntities: Map<UUID, Map<UUID, Set<Any>>>,
            associationEntities: Map<UUID, Map<UUID, Set<Any>>>,
            neighborEntities: Map<UUID, Map<UUID, Set<Any>>>,
            edges: Map<UUID, List<Pair<UUID, UUID>>>,
            propertyTypes: Map<UUID, PropertyType>
    ): List<FilteredNeighborsRankingAggregationResult> {
        //For each entity compute the association and neighbor aggregations.
        return srcEntities.mapNotNull { (entityKeyId, _) ->
            val neighbors = edges[entityKeyId] ?: return@mapNotNull FilteredNeighborsRankingAggregationResult(
                    entityKeyId,
                    0.0,
                    authorizedFilteredNeighborsRanking.filteredNeighborsRanking.associationTypeId,
                    authorizedFilteredNeighborsRanking.filteredNeighborsRanking.neighborTypeId,
                    EntityAggregationResult(mapOf(), mapOf()),
                    EntityAggregationResult(mapOf(), mapOf())
            )

            val (associationsEntityKeyIds, neighborEntityKeyIds) = neighbors.unzip()

            val associationAggregationResult = computeAggregationSlice(
                    authorizedFilteredNeighborsRanking,
                    associationsEntityKeyIds,
                    associationEntities,
                    propertyTypes,
                    true
            )

            val neighborAggregationResult = computeAggregationSlice(
                    authorizedFilteredNeighborsRanking,
                    neighborEntityKeyIds,
                    neighborEntities,
                    propertyTypes
            )


            var weight = authorizedFilteredNeighborsRanking.filteredNeighborsRanking.countWeight.orElse(1.0)

            //TODO: Consider properly count edges.
            var score = weight * neighborEntityKeyIds.size
            score += (associationAggregationResult.scorable.values.sum() + neighborAggregationResult.scorable.values.sum())

            FilteredNeighborsRankingAggregationResult(
                    entityKeyId,
                    score,
                    authorizedFilteredNeighborsRanking.filteredNeighborsRanking.associationTypeId,
                    authorizedFilteredNeighborsRanking.filteredNeighborsRanking.neighborTypeId,
                    associationAggregationResult,
                    neighborAggregationResult
            )
        }
    }

    private fun computeAggregationSlice(
            authorizedFilteredNeighborsRanking: AuthorizedFilteredNeighborsRanking,
            entityKeyIds: Collection<UUID>,
            entities: Map<UUID, Map<UUID, Set<Any>>>,
            propertyTypes: Map<UUID, PropertyType>,
            associationAggregation: Boolean = false
    ): EntityAggregationResult {
        val scorable = mutableMapOf<UUID, Double>()
        val passthrough = mutableMapOf<UUID, Comparable<*>>()

        //Calculate all the aggregations requested for this filtered neighbors ranking
        if (associationAggregation) {
            authorizedFilteredNeighborsRanking.filteredNeighborsRanking
                    .associationAggregations
        } else {
            authorizedFilteredNeighborsRanking.filteredNeighborsRanking
                    .neighborTypeAggregations
        }.forEach { (propertyTypeId, weightedRankingAggregation) ->
            val weight = weightedRankingAggregation.weight
            when (propertyTypes.getValue(propertyTypeId).datatype) {
                EdmPrimitiveTypeKind.GeographyPoint -> {
                    val values = entityKeyIds.mapNotNull {
                        entities[it]?.get(
                                propertyTypeId
                        )?.first() as String?
                    }
                    if (values.isEmpty()) return@forEach
                    when (weightedRankingAggregation.type) {
                        AggregationType.COUNT -> scorable[propertyTypeId] = values.count().toDouble() * weight
                        else -> throw InvalidParameterException(
                                "Unrecognized aggregation type for GeographyPoint."
                        )
                    }
                }
                EdmPrimitiveTypeKind.Int64 -> {
                    val values = entityKeyIds.mapNotNull { entities[it]?.get(propertyTypeId)?.first() as Long? }
                    if (values.isEmpty()) return@forEach
                    when (weightedRankingAggregation.type) {
                        AggregationType.AVG -> scorable[propertyTypeId] = values.average() * weight
                        AggregationType.MIN -> scorable[propertyTypeId] = values.min()!!.toDouble() * weight
                        AggregationType.MAX -> scorable[propertyTypeId] = values.max()!!.toDouble() * weight
                        AggregationType.COUNT -> scorable[propertyTypeId] = values.count().toDouble() * weight
                        AggregationType.SUM -> scorable[propertyTypeId] = values.sum().toDouble() * weight
                        else -> throw InvalidParameterException("Unrecognized aggregation type for Long.")
                    }
                }
                EdmPrimitiveTypeKind.Guid -> {
                    val values = entityKeyIds.mapNotNull {
                        entities[it]?.get(
                                propertyTypeId
                        )?.first() as String?
                    }
                            .map { UUID.fromString(it) }
                    if (values.isEmpty()) return@forEach
                    when (weightedRankingAggregation.type) {
                        AggregationType.MIN -> passthrough[propertyTypeId] = values.min()!!
                        AggregationType.MAX -> passthrough[propertyTypeId] = values.max()!!
                        AggregationType.COUNT -> scorable[propertyTypeId] = values.count().toDouble() * weight
                        else -> throw InvalidParameterException("Unrecognized aggregation type for Guid")
                    }
                }
                EdmPrimitiveTypeKind.SByte, EdmPrimitiveTypeKind.Byte -> {
                    val values = entityKeyIds.mapNotNull { entities[it]?.get(propertyTypeId)?.first() as Byte? }
                    if (values.isEmpty()) return@forEach
                    when (weightedRankingAggregation.type) {
                        AggregationType.AVG -> scorable[propertyTypeId] = values.average() * weight
                        AggregationType.MIN -> scorable[propertyTypeId] = values.min()!!.toDouble() * weight
                        AggregationType.MAX -> scorable[propertyTypeId] = values.max()!!.toDouble() * weight
                        AggregationType.COUNT -> scorable[propertyTypeId] = values.count().toDouble() * weight
                        AggregationType.SUM -> scorable[propertyTypeId] = values.sum().toDouble() * weight
                        else -> throw InvalidParameterException("Unrecognized aggregation type for (S)Byte.")
                    }
                }
                EdmPrimitiveTypeKind.Int16 -> {
                    val values = entityKeyIds.mapNotNull {
                        entities[it]?.get(
                                propertyTypeId
                        )?.first() as Short?
                    }
                    if (values.isEmpty()) return@forEach
                    when (weightedRankingAggregation.type) {
                        AggregationType.AVG -> scorable[propertyTypeId] = values.average() * weight
                        AggregationType.MIN -> scorable[propertyTypeId] = values.min()!!.toDouble() * weight
                        AggregationType.MAX -> scorable[propertyTypeId] = values.max()!!.toDouble() * weight
                        AggregationType.COUNT -> scorable[propertyTypeId] = values.count().toDouble() * weight
                        AggregationType.SUM -> scorable[propertyTypeId] = values.sum().toDouble() * weight
                        else -> throw InvalidParameterException("Unrecognized aggregation type for Int16.")
                    }
                }
                EdmPrimitiveTypeKind.Int32 -> {
                    val values = entityKeyIds.mapNotNull { entities[it]?.get(propertyTypeId)?.first() as Int? }
                    if (values.isEmpty()) return@forEach
                    when (weightedRankingAggregation.type) {
                        AggregationType.AVG -> scorable[propertyTypeId] = values.average() * weight
                        AggregationType.MIN -> scorable[propertyTypeId] = values.min()!!.toDouble() * weight
                        AggregationType.MAX -> scorable[propertyTypeId] = values.max()!!.toDouble() * weight
                        AggregationType.COUNT -> scorable[propertyTypeId] = values.count().toDouble() * weight
                        AggregationType.SUM -> scorable[propertyTypeId] = values.sum().toDouble() * weight
                        else -> throw InvalidParameterException("Unrecognized aggregation type for Int32.")
                    }

                }
                EdmPrimitiveTypeKind.Duration -> {
                    val values = entityKeyIds.mapNotNull { entities[it]?.get(propertyTypeId)?.first() as Long? }
                    when (weightedRankingAggregation.type) {
                        AggregationType.AVG -> scorable[propertyTypeId] = values.average() * weight
                        AggregationType.MIN -> scorable[propertyTypeId] = values.min()!!.toDouble() * weight
                        AggregationType.MAX -> scorable[propertyTypeId] = values.max()!!.toDouble() * weight
                        AggregationType.COUNT -> scorable[propertyTypeId] = values.count().toDouble() * weight
                        AggregationType.SUM -> scorable[propertyTypeId] = values.sum().toDouble() * weight
                        else -> throw InvalidParameterException("Unrecognized aggregation type for Duration.")
                    }
                }
                EdmPrimitiveTypeKind.Date -> {
                    val values = entityKeyIds
                            .mapNotNull { entities[it]?.get(propertyTypeId)?.first() as String? }
                            .map(LocalDate::parse)
                    if (values.isEmpty()) return@forEach
                    when (weightedRankingAggregation.type) {
                        AggregationType.MIN -> passthrough[propertyTypeId] = values.min()!!
                        AggregationType.MAX -> passthrough[propertyTypeId] = values.max()!!
                        AggregationType.COUNT -> scorable[propertyTypeId] = values.count().toDouble() * weight
                        else -> throw InvalidParameterException("Unrecognized aggregation type for Date.")
                    }
                }
                EdmPrimitiveTypeKind.TimeOfDay -> {
                    val values = entityKeyIds
                            .mapNotNull { entities[it]?.get(propertyTypeId)?.first() as String? }
                            .map(LocalTime::parse)
                    if (values.isEmpty()) return@forEach
                    when (weightedRankingAggregation.type) {
                        AggregationType.MIN -> passthrough[propertyTypeId] = values.min()!!
                        AggregationType.MAX -> passthrough[propertyTypeId] = values.max()!!
                        AggregationType.COUNT -> scorable[propertyTypeId] = values.count().toDouble() * weight
                        else -> throw InvalidParameterException("Unrecognized aggregation type for TimeOfDay.")
                    }
                }
                EdmPrimitiveTypeKind.DateTimeOffset -> {
                    val values = entityKeyIds
                            .mapNotNull { entities[it]?.get(propertyTypeId)?.first() as String? }
                            .map(OffsetDateTime::parse)
                    if (values.isEmpty()) return@forEach
                    when (weightedRankingAggregation.type) {
                        AggregationType.MIN -> passthrough[propertyTypeId] = values.min()!!
                        AggregationType.MAX -> passthrough[propertyTypeId] = values.max()!!
                        AggregationType.COUNT -> scorable[propertyTypeId] = values.count().toDouble() * weight
                        else -> throw InvalidParameterException(
                                "Unrecognized aggregation type for DateTimeOffset."
                        )
                    }
                }
                EdmPrimitiveTypeKind.Double -> {
                    val values = entityKeyIds.mapNotNull {
                        val v = entities[it]?.get(
                                propertyTypeId
                        )?.first()
                        //TODO: Address jackson serialization causing unexpected data types here.
                        if (v != null && v is Int) {
                            v.toDouble() as Double?
                        } else {
                            v as Double?
                        }
                    }
                    if (values.isEmpty()) return@forEach
                    when (weightedRankingAggregation.type) {
                        AggregationType.AVG -> scorable[propertyTypeId] = values.average() * weight
                        AggregationType.MIN -> scorable[propertyTypeId] = values.min()!! * weight
                        AggregationType.MAX -> scorable[propertyTypeId] = values.max()!! * weight
                        AggregationType.COUNT -> scorable[propertyTypeId] = values.count().toDouble() * weight
                        AggregationType.SUM -> scorable[propertyTypeId] = values.sum()
                        else -> throw InvalidParameterException("Unrecognized aggregation type for Double.")
                    }
                }
                EdmPrimitiveTypeKind.Binary -> {
                    val values = entityKeyIds.mapNotNull {
                        entities[it]?.get(
                                propertyTypeId
                        )?.first() as String?
                    }
                    if (values.isEmpty()) return@forEach
                    when (weightedRankingAggregation.type) {
                        AggregationType.COUNT -> scorable[propertyTypeId] = values.count().toDouble() * weight
                        else -> throw InvalidParameterException("Unrecognized aggregation type for Binary.")
                    }
                }
                EdmPrimitiveTypeKind.Boolean -> {
                    val values = entityKeyIds.mapNotNull { entities[it]?.get(propertyTypeId)?.first() as Long? }
                    if (values.isEmpty()) return@forEach
                    when (weightedRankingAggregation.type) {
                        AggregationType.COUNT -> scorable[propertyTypeId] = values.count().toDouble() * weight
                        else -> throw InvalidParameterException("Unrecognized aggregation type for Boolean.")
                    }
                }
                EdmPrimitiveTypeKind.String -> {
                    val values = entityKeyIds.mapNotNull {
                        entities[it]?.get(
                                propertyTypeId
                        )?.first() as String?
                    }
                    if (values.isEmpty()) return@forEach
                    when (weightedRankingAggregation.type) {
                        AggregationType.MIN -> passthrough[propertyTypeId] = values.min()!!
                        AggregationType.MAX -> passthrough[propertyTypeId] = values.max()!!
                        AggregationType.COUNT -> scorable[propertyTypeId] = values.count().toDouble() * weight
                        else -> throw InvalidParameterException("Unrecognized aggregation type for String.")
                    }
                }
                else -> throw InvalidParameterException("Unsupported data type for aggregation.")
            }
        }
        return EntityAggregationResult(scorable, passthrough)
    }


    @VisibleForTesting
    fun buildTopEntitiesQuery(
            limit: Int,
            entitySetIds: Set<UUID>,
            filteredRankings: List<AuthorizedFilteredNeighborsRanking>,
            linked: Boolean,
            linkingEntitySetId: Optional<UUID>
    ): String {
        val joinColumns = if (linked) LINKING_ID.name else "$SELF_ENTITY_SET_ID, $SELF_ENTITY_KEY_ID"

        val associationColumns = filteredRankings.mapIndexed { index, authorizedFilteredRanking ->
            authorizedFilteredRanking.filteredNeighborsRanking.associationAggregations.map {
                val column = aggregationAssociationColumnName(index, it.key)
                "${it.value.weight}*COALESCE($column,0)"
            }
        }.flatten()

        val entityColumns = filteredRankings.mapIndexed { index, authorizedFilteredRanking ->
            authorizedFilteredRanking.filteredNeighborsRanking.neighborTypeAggregations.map {
                val column = aggregationEntityColumnName(index, it.key)
                "${it.value.weight}*COALESCE($column,0)"
            }
        }.flatten()


        val countColumns = filteredRankings.mapIndexed { index, authorizedFilteredRanking ->
            authorizedFilteredRanking.filteredNeighborsRanking.countWeight.map {
                "$it*COALESCE(${associationCountColumnName(
                        index
                )},0)"
            }
                    .orElse("")
        }.filter(String::isNotBlank)


        val scoreColumn = (associationColumns + entityColumns + countColumns)
                .joinToString("+", prefix = "(", postfix = ") as score")
        /*
         * Build the SQL for the association joins.
         */
        val associationSql =
                filteredRankings.mapIndexed { index, authorizedFilteredRanking ->
                    val tableSql = buildAssociationTable(
                            index, entitySetIds, authorizedFilteredRanking, linked
                    )
                    //We are guaranteed at least one association for a valid top utilizers request

                    if (index == 0) {
                        val selectedColumns =
                                if (linked) {
                                    " *, UUID('${linkingEntitySetId.get()}') as $SELF_ENTITY_SET_ID, ${LINKING_ID.name} as $SELF_ENTITY_KEY_ID, $scoreColumn "
                                } else {
                                    " *,$scoreColumn "
                                }
                        "SELECT $selectedColumns FROM ($tableSql) as assoc_table$index "

                    } else {
                        "FULL OUTER JOIN ($tableSql) as assoc_table$index USING($joinColumns) "
                    }
                }.joinToString("\n")

        val entitiesSql = filteredRankings.mapIndexed { index, authorizedFilteredRanking ->
            val tableSql = buildEntityTable(
                    index, entitySetIds, authorizedFilteredRanking, linked
            )
            "FULL OUTER JOIN ($tableSql) as entity_table$index USING($joinColumns) "
        }.joinToString("\n")
        val sql = "$associationSql \n$entitiesSql \nORDER BY score DESC \nLIMIT $limit"

        return sql
    }


    @Deprecated("Edges table queries need update")
    private fun topEntitiesWorker(
            limit: Int, entitySetId: UUID, srcFilters: SetMultimap<UUID, UUID>,
            dstFilters: SetMultimap<UUID, UUID>
    ): Stream<Pair<EntityDataKey, Long>> {
        val countColumn = "total_count"
        val query = getTopUtilizersSql(entitySetId, srcFilters, dstFilters, limit)
        return PostgresIterable(
                Supplier {
                    val connection = reader.connection
                    val stmt = connection.createStatement()
                    logger.info("Executing top utilizer query: {}", query)
                    val rs = stmt.executeQuery(query)
                    StatementHolder(connection, stmt, rs)
                },
                Function<ResultSet, Pair<EntityDataKey, Long>> {
                    ResultSetAdapters.entityDataKey(it) to it.getLong(countColumn)
                }
        ).stream()
    }

    override fun getNeighborEntitySets(
            entitySetIds: Set<UUID>
    ): List<NeighborSets> {
        val neighbors: MutableList<NeighborSets> = ArrayList()

        val query = "SELECT DISTINCT ${SRC_ENTITY_SET_ID.name},${EDGE_ENTITY_SET_ID.name}, ${DST_ENTITY_SET_ID.name} " +
                "FROM ${E.name} " +
                "WHERE ( ${SRC_ENTITY_SET_ID.name} = ANY(?) OR ${DST_ENTITY_SET_ID.name} = ANY(?) ) " +
                "AND ${VERSION.name} > 0"
        val connection = reader.connection
        connection.use {
            val ps = connection.prepareStatement(query)
            ps.use {
                val entitySetIdsArr = PostgresArrays.createUuidArray(
                        connection, entitySetIds
                )
                ps.setArray(1, entitySetIdsArr)
                ps.setArray(2, entitySetIdsArr)
                val rs = ps.executeQuery()
                while (rs.next()) {
                    val srcEntitySetId = rs.getObject(SRC_ENTITY_SET_ID.name) as UUID
                    val edgeEntitySetId = rs.getObject(EDGE_ENTITY_SET_ID.name) as UUID
                    val dstEntitySetId = rs.getObject(DST_ENTITY_SET_ID.name) as UUID
                    neighbors.add(
                            NeighborSets(srcEntitySetId, edgeEntitySetId, dstEntitySetId)
                    )
                }
            }
        }
        return neighbors
    }

    override fun getEdgeEntitySetsConnectedToEntities(
            entitySetId: UUID, entityKeyIds: Set<UUID>
    ): Set<UUID> {
        val query = "SELECT DISTINCT ${EDGE_ENTITY_SET_ID.name} " +
                "FROM ${E.name} " +
                "WHERE ($SRC_IDS_SQL) OR ($DST_IDS_SQL) "

        return PostgresIterable(
                Supplier {
                    val connection = reader.connection
                    val entityKeyIdArr = PostgresArrays.createUuidArray(
                            connection, entityKeyIds
                    )

                    val ps = connection.prepareStatement(query)
                    ps.setArray(1, entityKeyIdArr)
                    ps.setObject(2, entitySetId)
                    ps.setArray(3, entityKeyIdArr)
                    ps.setObject(4, entitySetId)
                    val rs = ps.executeQuery()
                    StatementHolder(connection, ps, rs)
                },
                Function<ResultSet, UUID> { ResultSetAdapters.edgeEntitySetId(it) }
        ).toSet()
    }


    override fun getEdgeEntitySetsConnectedToEntitySet(
            entitySetId: UUID
    ): Set<UUID> {
        val query = "SELECT DISTINCT ${EDGE_ENTITY_SET_ID.name} " +
                "FROM ${E.name} " +
                "WHERE ${SRC_ENTITY_SET_ID.name} = ? OR ${DST_ENTITY_SET_ID.name} = ?"

        return BasePostgresIterable(
                Supplier {
                    val connection = reader.connection

                    val ps = connection.prepareStatement(query)
                    ps.setObject(1, entitySetId)
                    ps.setObject(2, entitySetId)
                    val rs = ps.executeQuery()
                    StatementHolder(connection, ps, rs)
                },
                { ResultSetAdapters.edgeEntitySetId(it) }
        ).toSet()
    }

    private fun buildAssociationTable(
            index: Int,
            selfEntitySetIds: Set<UUID>,
            authorizedFilteredRanking: AuthorizedFilteredNeighborsRanking,
            linked: Boolean
    ): String {
        val esEntityKeyIds = entitySetManager
                .getEntitySetIdsOfType(
                        authorizedFilteredRanking.filteredNeighborsRanking.associationTypeId
                )
                .map { it to Optional.empty<Set<UUID>>() }
                .toMap()
        val associationPropertyTypes = authorizedFilteredRanking.associationPropertyTypes
        //This will read all the entity sets of the same type all at once applying filters.
        val dataSql = selectEntitySetWithCurrentVersionOfPropertyTypes(
                esEntityKeyIds,
                associationPropertyTypes.mapValues {
                    quote(
                            it.value.type.fullQualifiedNameAsString
                    )
                },
                authorizedFilteredRanking.filteredNeighborsRanking.associationAggregations.keys,
                authorizedFilteredRanking.associationSets,
                authorizedFilteredRanking.filteredNeighborsRanking.associationFilters,
                setOf(),
                associationPropertyTypes.mapValues { it.value.datatype == EdmPrimitiveTypeKind.Binary },
                false,
                false
        )

        val joinColumns = entityKeyIdColumns
        val groupingColumns = if (linked) LINKING_ID.name else "$SELF_ENTITY_SET_ID, $SELF_ENTITY_KEY_ID"

        val spineSql = buildSpineSql(
                selfEntitySetIds, authorizedFilteredRanking, linked, true
        )

        val aggregationColumns =
                authorizedFilteredRanking.filteredNeighborsRanking.associationAggregations
                        .mapValues {
                            val fqn = quote(
                                    associationPropertyTypes.getValue(
                                            it.key
                                    ).type.fullQualifiedNameAsString
                            )
                            val alias = aggregationAssociationColumnName(index, it.key)
                            "${it.value.type.name}(COALESCE($fqn[1],0)) as $alias"
                        }.values.joinToString(",")
        val countAlias = associationCountColumnName(index)
        val allColumns = listOf(
                groupingColumns, aggregationColumns, "count(*) as $countAlias"
        )
                .filter(String::isNotBlank)
                .joinToString(",")
        val nullCheck = if (linked) "WHERE ${LINKING_ID.name} IS NOT NULL" else ""
        return "SELECT $allColumns " +
                "FROM ($spineSql $nullCheck) as spine " +
                "INNER JOIN ($dataSql) as data USING($joinColumns) " +
                "GROUP BY ($groupingColumns)"
    }


    private fun buildEntityTable(
            index: Int,
            selfEntitySetIds: Set<UUID>,
            authorizedFilteredRanking: AuthorizedFilteredNeighborsRanking,
            linked: Boolean
    ): String {
        val esEntityKeyIds = entitySetManager
                .getEntitySetIdsOfType(
                        authorizedFilteredRanking.filteredNeighborsRanking.neighborTypeId
                )
                .map { it to Optional.empty<Set<UUID>>() }
                .toMap()
        val entitySetPropertyTypes = authorizedFilteredRanking.entitySetPropertyTypes
        //This will read all the entity sets of the same type all at once applying filters.
        val dataSql = selectEntitySetWithCurrentVersionOfPropertyTypes(
                esEntityKeyIds,
                entitySetPropertyTypes.mapValues {
                    quote(
                            it.value.type.fullQualifiedNameAsString
                    )
                },
                authorizedFilteredRanking.filteredNeighborsRanking.neighborTypeAggregations.keys,
                authorizedFilteredRanking.entitySets,
                authorizedFilteredRanking.filteredNeighborsRanking.neighborFilters,
                setOf(),
                entitySetPropertyTypes.mapValues { it.value.datatype == EdmPrimitiveTypeKind.Binary },
                linking = false,
                omitEntitySetId = false
        )


        val joinColumns = entityKeyIdColumns
        val groupingColumns = if (linked) LINKING_ID.name else "$SELF_ENTITY_SET_ID, $SELF_ENTITY_KEY_ID"

        val spineSql = buildSpineSql(
                selfEntitySetIds, authorizedFilteredRanking, linked, false
        )

        val aggregationColumns =
                authorizedFilteredRanking.filteredNeighborsRanking.neighborTypeAggregations
                        .mapValues {
                            val fqn = quote(
                                    entitySetPropertyTypes.getValue(
                                            it.key
                                    ).type.fullQualifiedNameAsString
                            )
                            val alias = aggregationEntityColumnName(index, it.key)
                            "${it.value.type.name}(COALESCE($fqn[1],0)) as $alias"
                        }.values.joinToString(",")
        val countAlias = entityCountColumnName(index)
        val allColumns = listOf(
                groupingColumns, aggregationColumns, "count(*) as $countAlias"
        )
                .filter(String::isNotBlank)
                .joinToString(",")
        val nullCheck = if (linked) "WHERE ${LINKING_ID.name} IS NOT NULL" else ""
        return "SELECT $allColumns " +
                "FROM ($spineSql $nullCheck) as spine " +
                "INNER JOIN ($dataSql) as data USING($joinColumns) " +
                "GROUP BY ($groupingColumns)"
    }
}

internal fun getLinkingId(linkingIds: Map<UUID, UUID>, entityKeyId: UUID): UUID = linkingIds.getValue(entityKeyId)

private val KEY_COLUMNS = E.primaryKey.map { col -> col.name }.toSet()

private val INSERT_COLUMNS = E.columns.map { it.name }.toSet()

/**
 * Builds the SQL query for top utilizers.
 *
 * filter keys are allowed association entity set ids
 *
 * filter values are allowed neighbor entity set ids
 *
 * The simple version of top utilizers should count the number of allowed neighbors.
 *
 * That is for each
 */
@Deprecated("Edges table queries need update")
internal fun getTopUtilizersSql(
        entitySetId: UUID,
        srcFilters: SetMultimap<UUID, UUID>,
        dstFilters: SetMultimap<UUID, UUID>,
        top: Int = 100
): String {

    return "SELECT ${PostgresColumn.ENTITY_SET_ID.name}, ${ID_VALUE.name}, (COALESCE(src_count,0) + COALESCE(dst_count,0)) as total_count " +
            "FROM (${getTopUtilizersFromSrc(entitySetId, srcFilters)}) as src_counts " +
            "FULL OUTER JOIN (${getTopUtilizersFromDst(entitySetId, dstFilters)}) as dst_counts " +
            "USING(${PostgresColumn.ENTITY_SET_ID.name}, ${ID_VALUE.name}) " +
//            "WHERE total_count IS NOT NULL " +
            "ORDER BY total_count DESC " +
            "LIMIT $top"

}

@Deprecated("Edges table queries need update")
internal fun getTopUtilizersFromSrc(entitySetId: UUID, filters: SetMultimap<UUID, UUID>): String {
    val countColumn = "src_count"
    return "SELECT ${SRC_ENTITY_SET_ID.name} as ${PostgresColumn.ENTITY_SET_ID.name}, ${SRC_ENTITY_KEY_ID.name} as ${ID_VALUE.name}, count(*) as $countColumn " +
            "FROM ${E.name} WHERE ${srcClauses(entitySetId, filters)} " +
            "GROUP BY (${PostgresColumn.ENTITY_SET_ID.name}, ${ID_VALUE.name}) "
}

@Deprecated("Edges table queries need update")
internal fun getTopUtilizersFromDst(entitySetId: UUID, filters: SetMultimap<UUID, UUID>): String {
    val countColumn = "dst_count"
    return "SELECT ${DST_ENTITY_SET_ID.name} as ${PostgresColumn.ENTITY_SET_ID.name}, ${DST_ENTITY_KEY_ID.name} as ${ID_VALUE.name}, count(*) as $countColumn " +
            "FROM ${E.name} WHERE ${dstClauses(entitySetId, filters)} " +
            "GROUP BY (${PostgresColumn.ENTITY_SET_ID.name}, ${ID_VALUE.name}) "
}


val EDGES_UPSERT_SQL = "INSERT INTO ${E.name} (${INSERT_COLUMNS.joinToString(",")}) " +
        "VALUES (${(INSERT_COLUMNS.indices).joinToString(",") { "?" }}) " +
        "ON CONFLICT (${KEY_COLUMNS.joinToString(",")}) " +
        "DO UPDATE SET ${VERSION.name} = EXCLUDED.${VERSION.name}, " +
        "${VERSIONS.name} = ${E.name}.${VERSIONS.name} || EXCLUDED.${VERSION.name}"


private val CLEAR_SQL = "UPDATE ${E.name} SET ${VERSION.name} = ?, ${VERSIONS.name} = ${VERSIONS.name} || ? WHERE "
private val DELETE_SQL = "DELETE FROM ${E.name} WHERE "
private val LOCK_SQL1 = "SELECT 1 FROM ${E.name} WHERE "
private const val LOCK_SQL2 = " FOR UPDATE"

private val VERTEX_FILTER_SQL = "${KEY_COLUMNS.joinToString(" = ? AND ")} = ? "

private val CLEAR_BY_VERTEX_SQL = "$CLEAR_SQL $VERTEX_FILTER_SQL"
private val DELETE_BY_VERTEX_SQL = "$DELETE_SQL $VERTEX_FILTER_SQL"
private val LOCK_BY_VERTEX_SQL = "$LOCK_SQL1 $VERTEX_FILTER_SQL $LOCK_SQL2"

private val NEIGHBORHOOD_OF_ENTITY_SET_SQL = "SELECT * FROM ${E.name} WHERE " +
        "( (${SRC_ENTITY_SET_ID.name} = ?) OR (${EDGE_ENTITY_SET_ID.name} = ?) OR (${DST_ENTITY_SET_ID.name} = ?) )"
private val NON_TOMBSTONED_NEIGHBORHOOD_OF_ENTITY_SET_SQL = "$NEIGHBORHOOD_OF_ENTITY_SET_SQL AND ${VERSION.name} > 0"

private val SRC_ID_SQL = "${SRC_ENTITY_KEY_ID.name} = ? AND ${SRC_ENTITY_SET_ID.name} = ?"
private val DST_ID_SQL = "${DST_ENTITY_KEY_ID.name} = ? AND ${DST_ENTITY_SET_ID.name} = ?"

private val SRC_IDS_SQL = "${SRC_ENTITY_KEY_ID.name} = ANY(?) AND ${SRC_ENTITY_SET_ID.name} = ?"
private val DST_IDS_SQL = "${DST_ENTITY_KEY_ID.name} = ANY(?) AND ${DST_ENTITY_SET_ID.name} = ?"
private val EDGE_IDS_SQL = "${EDGE_ENTITY_KEY_ID.name} = ANY(?) AND ${EDGE_ENTITY_SET_ID.name} = ?"

private val SRC_IDS_AND_ENTITY_SETS_SQL = "${SRC_ENTITY_KEY_ID.name} = ANY(?) AND ${SRC_ENTITY_SET_ID.name} = ANY(?)"
private val DST_IDS_AND_ENTITY_SETS_SQL = "${DST_ENTITY_KEY_ID.name} = ANY(?) AND ${DST_ENTITY_SET_ID.name} = ANY(?)"

/**
 * Loads edges where either the source, destination, or association matches a set of entityKeyIds from a specific entity set
 *
 * 1. entityKeyIds
 * 2. entitySetId
 * 3. entityKeyIds
 * 4. entitySetId
 * 5. entityKeyIds
 * 6. entitySetId
 */
private val BULK_NEIGHBORHOOD_SQL = "SELECT * FROM ${E.name} WHERE (($SRC_IDS_SQL) OR ($DST_IDS_SQL) OR ($EDGE_IDS_SQL))"
private val BULK_NON_TOMBSTONED_NEIGHBORHOOD_SQL = "$BULK_NEIGHBORHOOD_SQL AND ${VERSION.name} > 0"

/**
 * Loads edges where either the source or destination matches an EntityDataKey
 *
 * 1. entityKeyId
 * 2. entitySetId
 * 3. entityKeyId
 * 4. entitySetId
 * 5. entityKeyId
 * 6. entitySetId
 */
private val NEIGHBORHOOD_SQL = "SELECT * FROM ${E.name} WHERE ($SRC_ID_SQL) OR ($DST_ID_SQL)"

internal fun getFilteredNeighborhoodSql(filter: EntityNeighborsFilter, multipleEntitySetIds: Boolean): String {

    var (srcSql, dstSql) = if (multipleEntitySetIds) {
        SRC_IDS_AND_ENTITY_SETS_SQL to DST_IDS_AND_ENTITY_SETS_SQL
    } else {
        SRC_IDS_SQL to DST_IDS_SQL
    }

    if (filter.dstEntitySetIds.isPresent) {
        if (filter.dstEntitySetIds.get().size > 0) {
            srcSql += " AND ( ${DST_ENTITY_SET_ID.name} IN (${filter.dstEntitySetIds.get().joinToString(
                    ","
            ) { "'$it'" }}))"
        } else {
            srcSql = "false AND $srcSql "
        }
    }

    if (filter.srcEntitySetIds.isPresent) {
        if (filter.srcEntitySetIds.get().size > 0) {
            dstSql += " AND ( ${SRC_ENTITY_SET_ID.name} IN (${filter.srcEntitySetIds.get().joinToString(
                    ","
            ) { "'$it'" }}))"
        } else {
            dstSql = "false AND $dstSql "
        }
    }

    if (filter.associationEntitySetIds.isPresent) {
        srcSql += " AND ( ${EDGE_ENTITY_SET_ID.name} IN (${filter.associationEntitySetIds.get().joinToString(
                ","
        ) { "'$it'" }}))"
        dstSql += " AND ( ${EDGE_ENTITY_SET_ID.name} IN (${filter.associationEntitySetIds.get().joinToString(
                ","
        ) { "'$it'" }}))"
    }

    return "SELECT * FROM ${E.name} WHERE (( $srcSql ) OR ( $dstSql )) AND ${VERSION.name} > 0"
}

private fun srcClauses(entitySetId: UUID, associationFilters: SetMultimap<UUID, UUID>): String {
    return associationClauses(
            SRC_ENTITY_SET_ID.name, entitySetId, DST_ENTITY_SET_ID.name, associationFilters
    )
}

private fun dstClauses(entitySetId: UUID, associationFilters: SetMultimap<UUID, UUID>): String {
    return associationClauses(
            DST_ENTITY_SET_ID.name, entitySetId, SRC_ENTITY_SET_ID.name, associationFilters
    )
}

private fun buildEdgeFilteringClause(
        selfEntitySetIds: Set<UUID>,
        authorizedFilteredRanking: AuthorizedFilteredNeighborsRanking,
        association: Boolean,
        isDst: Boolean
): String {
    val authorizedAssociationEntitySets = authorizedFilteredRanking.associationSets.keys
    val authorizedEntitySets = authorizedFilteredRanking.entitySets.keys

    val associationsClause =
            "${EDGE_ENTITY_SET_ID.name} IN (${authorizedAssociationEntitySets.joinToString(",") { "'$it'" }})"

    val entitySetColumn = if (isDst) {
        DST_ENTITY_SET_ID.name
    } else {
        SRC_ENTITY_SET_ID.name
    }

    val selfEntitySetColumn = if (isDst) {
        SRC_ENTITY_SET_ID.name
    } else {
        DST_ENTITY_SET_ID.name
    }

    val entitySetsClause =
            "$entitySetColumn IN (${authorizedEntitySets.joinToString(",") { "'$it'" }}) " +
                    "AND $selfEntitySetColumn IN (${selfEntitySetIds.joinToString(",") { "'$it'" }})"

    return "($associationsClause AND $entitySetsClause)"
}

private fun buildSpineSql(
        selfEntitySetIds: Set<UUID>,
        authorizedFilteredRanking: AuthorizedFilteredNeighborsRanking,
        linked: Boolean,
        association: Boolean
): String {
    val isDst = authorizedFilteredRanking.filteredNeighborsRanking.dst


    val baseEntityColumnsSql = if (association) {
        // Order on which we select is {edge, src, dst}, EDGE IdType
        if (isDst) { // select src and edge
            "${SRC_ENTITY_SET_ID.name} as $SELF_ENTITY_SET_ID, ${EDGE_COMP_1.name} as $SELF_ENTITY_KEY_ID, " +
                    "${EDGE_ENTITY_SET_ID.name} as ${PostgresColumn.ENTITY_SET_ID.name}, ${ID_VALUE.name} as ${ID_VALUE.name}"
        } else { // select dst and edge
            "${DST_ENTITY_SET_ID.name} as $SELF_ENTITY_SET_ID, ${EDGE_COMP_2.name} as $SELF_ENTITY_KEY_ID, " +
                    "${EDGE_ENTITY_SET_ID.name} as ${PostgresColumn.ENTITY_SET_ID.name}, ${ID_VALUE.name} as ${ID_VALUE.name}"
        }
    } else {
        // Order on which we select is {src, dst, edge}, DST IdType
        if (isDst) { // select src and dst
            "${SRC_ENTITY_SET_ID.name} as $SELF_ENTITY_SET_ID, ${EDGE_COMP_2.name} as $SELF_ENTITY_KEY_ID, " +
                    "${DST_ENTITY_SET_ID.name} as ${PostgresColumn.ENTITY_SET_ID.name}, ${ID_VALUE.name} as ${ID_VALUE.name}"
        } else { // Order on which we select is {src, dst, edge}, SRC IdType
            // select dst and src
            "${DST_ENTITY_SET_ID.name} as $SELF_ENTITY_SET_ID, ${EDGE_COMP_1.name} as $SELF_ENTITY_KEY_ID, " +
                    "${SRC_ENTITY_SET_ID.name} as ${PostgresColumn.ENTITY_SET_ID.name}, ${ID_VALUE.name} as ${ID_VALUE.name}"
        }
    }

    val edgeClause = buildEdgeFilteringClause(selfEntitySetIds, authorizedFilteredRanking, association, isDst)
    val idSql = "SELECT ${PostgresColumn.ENTITY_SET_ID.name} as $SELF_ENTITY_SET_ID, ${PostgresColumn.ID.name} as $SELF_ENTITY_KEY_ID, ${LINKING_ID.name} FROM ${ENTITY_KEY_IDS.name}"
    val spineSql = if (linked) {
        "SELECT edges.*, ${LINKING_ID.name} FROM (SELECT DISTINCT $baseEntityColumnsSql FROM edges WHERE $edgeClause) as edges " +
                "LEFT JOIN ($idSql) as ${ENTITY_KEY_IDS.name} USING ($SELF_ENTITY_SET_ID,$SELF_ENTITY_KEY_ID)"
    } else {
        "SELECT DISTINCT $baseEntityColumnsSql FROM edges WHERE $edgeClause"
    }

    return spineSql
}

/**
 * Generates an association clause for querying the edges table.
 * @param entitySetColumn The column to use for filtering allowed entity set ids.
 * @param entitySetId The entity set id for which the aggregation will be performed.
 * @param neighborColumn The column for the neighbors that will be counted.
 * @param associationFilters A multimap from association entity set ids to allowed neighbor entity set ids.
 */
private fun associationClauses(
        entitySetColumn: String, entitySetId: UUID, neighborColumn: String,
        associationFilters: SetMultimap<UUID, UUID>
): String {
    if (associationFilters.isEmpty) {
        return " false "
    }
    return Multimaps
            .asMap(associationFilters)
            .asSequence()
            .map {
                "($entitySetColumn = '$entitySetId' AND ${EDGE_ENTITY_SET_ID.name} = '${it.key}' " +
                        "AND $neighborColumn IN (${it.value.joinToString(",") { "'$it'" }}) ) "
            }
            .joinToString(" OR ")
}

const val ASSOC = "assoc"
const val ENTITY = "entity"
internal fun buildAggregationColumnMap(
        index: Int,
        propertyTypes: Map<UUID, PropertyType>,
        aggregations: Map<UUID, WeightedRankingAggregation>,
        type: String
): Map<UUID, String> {
    return aggregations
            .mapValues {

                when (type) {
                    ASSOC -> aggregationAssociationColumnName(index, it.key).replace("\"", "")
                    ENTITY -> aggregationEntityColumnName(index, it.key).replace("\"", "")
                    else -> throw IllegalStateException("Unsupported aggregation type: $type.")
                }
            }
}

internal fun aggregationAssociationColumnName(index: Int, propertyTypeId: UUID): String {
    return quote("${ASSOC}_${index}_$propertyTypeId")
}

internal fun aggregationEntityColumnName(index: Int, propertyTypeId: UUID): String {
    return quote("${ENTITY}_${index}_$propertyTypeId")
}

internal fun entityCountColumnName(index: Int): String {
    return "${ENTITY}_${index}_count"
}

internal fun associationCountColumnName(index: Int): String {
    return "${ASSOC}_${index}_count"
}

fun bindColumnsForEdge(
        ps: PreparedStatement,
        dataEdgeKey: DataEdgeKey,
        version: Long,
        versions: java.sql.Array,
        partitionsInfoByEntitySet: Map<UUID, List<Int>>
) {

    val edk = dataEdgeKey.src
    val partitions = partitionsInfoByEntitySet.getValue(edk.entitySetId)

    var index = 1

    ps.setObject(index++,
                 getPartition(edk.entityKeyId, partitions)
    )
    ps.setObject(index++, dataEdgeKey.src.entitySetId)
    ps.setObject(index++, dataEdgeKey.src.entityKeyId)
    ps.setObject(index++, dataEdgeKey.dst.entitySetId)
    ps.setObject(index++, dataEdgeKey.dst.entityKeyId)
    ps.setObject(index++, dataEdgeKey.edge.entitySetId)
    ps.setObject(index++, dataEdgeKey.edge.entityKeyId)
    ps.setLong(index++, version)
    ps.setArray(index++, versions)
    ps.addBatch()
}
