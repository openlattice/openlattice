package com.openlattice.graph.processing

import com.google.common.base.Stopwatch
import com.google.common.collect.Multimaps
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.core.IMap
import com.hazelcast.query.Predicate
import com.hazelcast.query.Predicates
import com.hazelcast.query.QueryConstants
import com.openlattice.data.EntityDataKey
import com.openlattice.data.storage.PostgresEntityDataQueryService
import com.openlattice.data.storage.entityKeyIdColumns
import com.openlattice.data.storage.selectEntitySetWithCurrentVersionOfPropertyTypes
import com.openlattice.datastore.services.EdmManager
import com.openlattice.edm.EntitySet
import com.openlattice.edm.type.EntityType
import com.openlattice.edm.type.PropertyType
import com.openlattice.graph.processing.processors.GraphProcessor
import com.openlattice.graph.processing.processors.NoneProcessor
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.postgres.DataTables.quote
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresTable.*
import com.openlattice.postgres.streams.PostgresIterable
import com.zaxxer.hikari.HikariDataSource
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.postgresql.util.PSQLException
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import java.sql.SQLException
import java.time.OffsetDateTime
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock

private const val EXPIRATION_MILLIS = 1000000L

class GraphProcessingService(
        private val edm: EdmManager,
        private val dqs: PostgresEntityDataQueryService,
        private val hds: HikariDataSource,
        hazelcastInstance: HazelcastInstance,
        processorsToRegister: Set<GraphProcessor>
) {
    private val propertyTypes: IMap<UUID, PropertyType> = hazelcastInstance.getMap(HazelcastMap.PROPERTY_TYPES.name)
    private val entityTypes: IMap<UUID, EntityType> = hazelcastInstance.getMap(HazelcastMap.ENTITY_TYPES.name)
    private val entitySets: IMap<UUID, EntitySet> = hazelcastInstance.getMap(HazelcastMap.ENTITY_SETS.name)
    //    private val processors = mutableMapOf<UUID, GraphProcessor>().withDefault { NoneProcessor(dqs) }
    private val processors = mutableSetOf<GraphProcessor>()
    private val processingLocks: IMap<UUID, Long> = hazelcastInstance.getMap(
            HazelcastMap.INDEXING_GRAPH_PROCESSING.name
    )

    private val propagationGraph: Map<Propagation, Set<Propagation>> = mutableMapOf()

    companion object {
        private val logger = LoggerFactory.getLogger(GraphProcessingService.javaClass)
    }

    init {
        processorsToRegister.forEach { register(it) }
        processingLocks.addIndex(QueryConstants.THIS_ATTRIBUTE_NAME.value(), true)
    }

    private val taskLock = ReentrantLock()

    @Scheduled(fixedRate = EXPIRATION_MILLIS)
    fun scavengeProcessingLocks() {
        processingLocks.removeAll(
                Predicates.lessThan(
                        QueryConstants.THIS_ATTRIBUTE_NAME.value(),
                        System.currentTimeMillis()
                ) as Predicate<UUID, Long>
        )
    }

    fun initialize() {
        /*
         * Need to to initialize all properties on neighbors of nodes that become active
         * That allows us to accumulate values into those properties. Easiest way to do this is to create selection of
         * entity keys neighboring active property types.
         *
         * Active entities are defined as entities where last_propagate < last_write and there are no neighboring edges
         * or associations for which last_propagate < last_write
         *
         * or at every step create a topological sort of property commputation graph
         */

    }

    fun step() {
        if (taskLock.tryLock()) {
            try {
                val entityTypes = getAllowedEntityTypes()

                while (getActiveCount() > 0) {
                    entityTypes.forEach {
                        val lastPropagateTime = OffsetDateTime.now()

                        val lockedEntitySets = edm.getEntitySetsOfType(it).filter {
                            tryLockEntitySet(
                                    it.id
                            )
                        }.map { it.id }
                        val activeEntities = getActiveEntities(lockedEntitySets)

                        //  entityset id / entity id / property id / value(s)
                        val entities = activeEntities.groupBy { it.entitySetId }
                                .mapValues {
                                    // entity key id / property id / value(s)
                                    val entityValues = dqs.getEntitiesById(it.key, edm.getPropertyTypesForEntitySet(it.key))
                                    entityValues.mapValues { Multimaps.asMap(it.value) }
                                }

                        processors[it]?.process(entities, lastPropagateTime)

                        lockedEntitySets.forEach(processingLocks::delete)
                    }
                }
            } finally {
                taskLock.unlock()
            }
        }
    }

    private fun propagate() {
        //Basically update all neighbors of where last_received > last_propagated. No filtering required because we
        //want to continuously be propagating the signal of this shouldn't be used for computation.

//        val pTypes = propertyTypes.values.map{ it.id to it }.toMap()
        var count = Int.MAX_VALUE
        while (count > 0) {
            count = propertyTypes.values.map {
                markIfPropagated(it)
            }.sum()
        }

    }

    private fun markIfPropagated(propertyType: PropertyType): Int {
        val queries = buildPropagationQueries()

        hds.connection.use { conn ->
            conn.autoCommit = false
            try {
                conn.createStatement().use { stmt ->
                    val w = Stopwatch.createStarted()
                    val propagationSize = queries.map(stmt::executeUpdate).sum()
                    logger.info("Propagated $propagationSize receives in ${w.elapsed(TimeUnit.MILLISECONDS)}ms")
                }
            } catch (e: SQLException) {
                logger.error("Unable to propagate information with sql queries: {}", queries)
            } finally {
                conn.autoCommit = true
            }
        }
    }

    private fun compute(): Int {

        processors.map { processor ->
            val entitySetIds = getEntitySets(processor.getInputs())
            val propertyTypes = getPropertyTypes(processor.getInputs())
            val outputEntitySetIds = getEntitySets(mapOf(processor.getOutputs()).mapValues { setOf(it.value) }).first()
            val outputPropertyType = getPropertyTypes(mapOf(processor.getOutputs()).mapValues { setOf(it.value) })
            val activeDataSql = buildGetActivePropertiesSql(entitySetIds, propertyTypes)
            hds.connection.use { conn ->
                conn.use {
                    it.use { stmt ->
                        stmt.use {
                            buildComputeQuery(outputEntitySetIds, outputPropertyType, processor.getSql() )
                        }
                    }

                }

            }
        }

    }

    private fun getPropertyTypes(inputs: Map<FullQualifiedName, Set<FullQualifiedName>>): Map<UUID, PropertyType> {
        return inputs.values
                .flatMap { it.map(edm::getPropertyType).map { it.id to it } }
                .toMap()
    }

    private fun getEntitySets(inputs: Map<FullQualifiedName, Set<FullQualifiedName>>): Set<UUID> {
        return inputs.keys
                .map(edm::getEntityType)
                .map(EntityType::getId)
                .flatMap(edm::getEntitySetsOfType)
                .map(EntitySet::getId)
                .toSet()
    }

    private fun getActiveEntities(entitySetIds: Collection<UUID>): PostgresIterable<EntityDataKey> {
        return dqs.getActiveEntitiesById(entitySetIds)
    }

    private fun getActiveCount(): Long {
        val count = dqs.getActiveEntitiesCount()
        logger.info("Active entity count: $count.")
        return count
    }

    private fun getAllowedEntityTypes(): Set<UUID> {
        return edm.entityTypes.map { it.id }.toSet()
    }

    private fun register(processor: GraphProcessor) {
        processor.getOutputTypes().map { edm.getPro }
        processor.handledEntityTypes().forEach {
            processors[it] = processor
        }
    }

    private fun tryLockEntitySet(entitySetId: UUID): Boolean {
        return processingLocks.putIfAbsent(entitySetId, System.currentTimeMillis() + EXPIRATION_MILLIS) == null
    }
}

//Using the information from the graph processors we know the progagation filters.
internal fun buildPropagationQueries(): List<String> {
    return buildPropertyTypeDataKeysNeedingPropagationSql()
            .map {
                "UPDATE ${PROPAGATION_STATE.name} SET ${LAST_RECEIVED.name} = now() " +
                        "FROM ($it) as propagations " +
                        "WHERE ${PROPAGATION_STATE.name}.${ENTITY_SET_ID.name} = propagations.$TARGET_ENTITY_SET_ID " +
                        " AND  ${PROPAGATION_STATE.name}.${ID_VALUE.name} = propagations.$TARGET_ENTITY_KEY_ID "
            }
}

internal fun buildPropGraphSql(dstAsColumn: String): String {
    return "SELECT DISTINCT ${SRC_ENTITY_SET_ID.name} as ${ENTITY_SET_ID.name}, ${SRC_PROPERTY_TYPE_ID.name} as ${PROPERTY_TYPE_ID.name}, " +
            "${DST_ENTITY_SET_ID.name} as $TARGET_ENTITY_SET_ID, ${DST_PROPERTY_TYPE_ID.name} as $TARGET_PROPERTY_TYPE_ID " +
            "FROM ${PROPAGATION_GRAPH.name} "
}

const val TARGET_PROPERTY_TYPE_ID = "target_property_type_id"
const val TARGET_ENTITY_SET_ID = "target_entity_set_id"
const val TARGET_ENTITY_KEY_ID = "target_entity_key_Id"

internal fun buildPropertyTypeDataKeysNeedingPropagationSql(): List<String> {
    //Only select entity sets participating in prop
    val srcEdgesSql = "SELECT ${SRC_ENTITY_SET_ID.name} as ${ENTITY_SET_ID.name}, ${SRC_ENTITY_KEY_ID.name} as ${ID_VALUE.name}, " +
            "${DST_ENTITY_SET_ID.name} as $TARGET_ENTITY_SET_ID, ${DST_ENTITY_KEY_ID.name} as $TARGET_ENTITY_KEY_ID" +
            "FROM ${EDGES.name}"

    val dstEdgesSql = "SELECT ${DST_ENTITY_SET_ID.name} as ${ENTITY_SET_ID.name}, ${DST_ENTITY_KEY_ID.name} as ${ID_VALUE.name}, " +
            "${SRC_ENTITY_SET_ID.name} as $TARGET_ENTITY_SET_ID, ${SRC_ENTITY_KEY_ID.name} as $TARGET_ENTITY_KEY_ID" +
            "FROM ${EDGES.name} "

    val edgeSrcEdgesSql = "SELECT ${EDGE_ENTITY_SET_ID.name} as ${ENTITY_SET_ID.name}, ${EDGE_ENTITY_KEY_ID.name} as ${ID_VALUE.name}, " +
            "${SRC_ENTITY_SET_ID.name} as $TARGET_ENTITY_SET_ID, ${SRC_ENTITY_KEY_ID.name} as $TARGET_ENTITY_KEY_ID" +
            "FROM ${EDGES.name} "

    val edgeDstEdgesSql = "SELECT ${EDGE_ENTITY_SET_ID.name} as ${ENTITY_SET_ID.name}, ${EDGE_ENTITY_KEY_ID.name} as ${ID_VALUE.name}, " +
            "${DST_ENTITY_SET_ID.name} as $TARGET_ENTITY_SET_ID, ${DST_ENTITY_KEY_ID.name} as $TARGET_ENTITY_KEY_ID" +
            "FROM ${EDGES.name} "


    val srcNeedsSql = buildNeedsSql(srcEdgesSql)
    val dstNeedsSql = buildNeedsSql(dstEdgesSql)
    val edgeSrcNeedsSql = buildNeedsSql(edgeSrcEdgesSql)
    val edgeDstNeedsSql = buildNeedsSql(edgeDstEdgesSql)

    return listOf(srcNeedsSql, dstNeedsSql, edgeSrcNeedsSql, edgeDstNeedsSql)
}

//A property is marked as propagted when it completes a compute step with no unpropagated neighbors/parents.
internal fun buildGetActivePropertiesSql(
        entitySetIds: Collection<UUID>, propertyTypeIds: Map<UUID, PropertyType>
): String {
    //Want to get property types across several entity sets of the same type.

    //This table can be used by entity sets to figure out where to insert new properties.

    //i.e if edge for entity exists and selected subset of properties can be propagate. Push them through to neighbors
    //The result of this query can be joined with edges and other
    return selectEntitySetWithCurrentVersionOfPropertyTypes(
            entitySetIds.map { it to Optional.empty<Set<UUID>>() }.toMap(),
            propertyTypeIds.mapValues { quote(it.value.type.fullQualifiedNameAsString) },
            propertyTypeIds.keys,
            entitySetIds.map { it to propertyTypeIds.keys }.toMap(),
            mapOf(),
            setOf(),
            false,
            propertyTypeIds.mapValues { it.value.datatype == EdmPrimitiveTypeKind.Binary },
            " AND last_propagate >= last_write"
    )

}

internal fun build

internal fun buildActiveSql(propertyType: UUID) {
    val needsPropagation = buildPropertyTypeDataKeysNeedingPropagationSql()
            .map {
                val sql = "SELECT ${ENTITY_SET_ID.name} as $TARGET_ENTITY_SET_ID, ${SRC_ENTITY_KEY_ID.name} as $TARGET_ENTITY_KEY_ID, ${PROPERTY_TYPE_ID.name} " +
                        "FROM ${PROPAGATION_STATE.name} " +
                        "WHERE ${LAST_PROPAGATE.name}  >= ${LAST_RECEIVED.name}"
                "SELECT * FROM ENTITY_KEY_"
            }
}

internal fun buildNeedsSql(edgesSql: String): String {
    val propGraphSql = buildPropGraphSql(TARGET_ENTITY_SET_ID)
    val propStateSql = "SELECT * FROM ${PROPAGATION_STATE.name} WHERE ${LAST_PROPAGATE.name} < ${LAST_RECEIVED.name}"
    return "SELECT * FROM ($propStateSql) as prop_state" +
            "INNER JOIN ($propGraphSql) prop_graph USING(${ENTITY_SET_ID.name},${PROPERTY_TYPE_ID.name}) " +
            "INNER JOIN ($edgesSql) as src_edges USING ($entityKeyIdColumns,$TARGET_ENTITY_SET_ID) "
}
