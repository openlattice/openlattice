package com.openlattice.graph.processing

import com.google.common.base.Preconditions.checkState
import com.google.common.base.Stopwatch
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.core.IMap
import com.hazelcast.query.QueryConstants
import com.openlattice.analysis.requests.ValueFilter
import com.openlattice.data.EntityDataKey
import com.openlattice.data.storage.PostgresEntityDataQueryService
import com.openlattice.data.storage.entityKeyIdColumns
import com.openlattice.data.storage.selectEntitySetWithCurrentVersionOfPropertyTypes
import com.openlattice.datastore.services.EdmManager
import com.openlattice.edm.EntitySet
import com.openlattice.edm.type.EntityType
import com.openlattice.edm.type.PropertyType
import com.openlattice.graph.processing.processors.GraphProcessor
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.postgres.DataTables
import com.openlattice.postgres.DataTables.LAST_WRITE
import com.openlattice.postgres.DataTables.quote
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresTable.EDGES
import com.openlattice.postgres.streams.PostgresIterable
import com.zaxxer.hikari.HikariDataSource
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.LoggerFactory
import java.lang.IllegalStateException
import java.sql.SQLException
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
    private val processors = mutableSetOf<GraphProcessor>()
    private val processingLocks: IMap<UUID, Long> = hazelcastInstance.getMap(
            HazelcastMap.INDEXING_GRAPH_PROCESSING.name
    )

    private val singleForwardPropagationGraph: MutableMap<Propagation, MutableSet<Propagation>> = mutableMapOf()

    companion object {
        private val logger = LoggerFactory.getLogger(GraphProcessingService::class.java)
    }

    init {
        processorsToRegister.forEach { register(it) }
        processingLocks.addIndex(QueryConstants.THIS_ATTRIBUTE_NAME.value(), true)
    }

    private val taskLock = ReentrantLock()

    fun step() {
        if (taskLock.tryLock()) {
            while(compute() > 0){}
            propagate()
        }
    }

    private fun propagate(): Int {
        //Basically update all neighbors of where last_received > last_propagated. No filtering required because we
        //want to continuously be propagating the signal of this shouldn't be used for computation.

        var count = Int.MAX_VALUE
        while (count > 0) {
            count = singleForwardPropagationGraph.map {
                markIfPropagated(it.key, it.value)
            }.sum()
        }
        return count
    }

    private fun markIfPropagated(input: Propagation, outputs: Set<Propagation>): Int {
        val entitySetIds = edm.getEntitySetsOfType(input.entityTypeId).map { it.id }
        val propertyTypes = this.propertyTypes.getAll(setOf(input.propertyTypeId))
        val outputEntitySetIds = outputs.flatMap { edm.getEntitySetsOfType(it.entityTypeId) }.map { it.id }
        val outputPropertyType = outputs.map { it.propertyTypeId }.toSet()
        val associationType = edm.getAssociationTypeSafe(input.entityTypeId) != null
        try {
            val queries = buildPropagationQueries(
                    outputEntitySetIds,
                    outputPropertyType,
                    entitySetIds,
                    propertyTypes,
                    associationType,
                    input.self
            )

            return hds.connection.use { conn ->
                conn.autoCommit = false
                try {
                    conn.createStatement().use { stmt ->
                        val w = Stopwatch.createStarted()
                        val propagationSize = queries.map(stmt::executeUpdate).sum()
                        logger.info("Propagated $propagationSize in ${w.elapsed(TimeUnit.MILLISECONDS)}ms")
                        return propagationSize
                    }
                } catch (e: SQLException) {
                    logger.error("Unable to propagate information with sql queries: {} ${System.lineSeparator()}$e", queries)
                    return 0
                } finally {
                    conn.autoCommit = true
                }
            }
        } catch(e:IllegalStateException) {
            logger.error("Couldn't propagate input entity type ${edm.getEntityTypeFqn(input.entityTypeId)}: $e")
        }
        return 0
    }

    private fun compute(): Int {
        val count = processors.map { processor ->
            val entitySetIds = getEntitySetsPerType(processor.getInputs())
            val propertyTypes = getPropertyTypes(processor.getInputs())
            val outputEntitySetIds = getEntitySetsPerType(mapOf(processor.getOutput()).mapValues { setOf(it.value) }).entries.first().value
            val outputPropertyType = getPropertyTypes(mapOf(processor.getOutput()).mapValues { setOf(it.value) })
            val associationType = edm.getAssociationTypeSafe(edm.getEntityType(processor.getOutput().first).id) != null
            val filters = processor.getFilters().values.flatMap { it.map{ edm.getPropertyTypeId(it.key) to setOf(it.value)} }.toMap()
            try{
                val computeQueries = buildComputeQueries(
                        processor.getSql(),
                        filters,
                        outputEntitySetIds,
                        outputPropertyType.values.first().first().id,
                        quote(outputPropertyType.values.first().first().type.fullQualifiedNameAsString),
                        entitySetIds,
                        propertyTypes,
                        associationType,
                        processor.isSelf())

                hds.connection.use { conn ->
                    conn.use {
                        try {
                            conn.createStatement().use { stmt ->
                                computeQueries.map(stmt::executeUpdate).sum()
                            }
                        } catch (e: SQLException) {
                            logger.error("Unable to compute aggregated values with queries: {} ${System.lineSeparator()}$e", computeQueries)
                             0
                        }
                    }
                }
            } catch(e:IllegalStateException) {
                logger.error("Couldn't compute property type ${processor.getOutput().second} of entity type ${processor.getOutput().first}: $e")
                 0
            }
        }.sum()

        return count
    }

    // entity type id -> setof (pt_id -> pt)
    private fun getPropertyTypes(inputs: Map<FullQualifiedName, Set<FullQualifiedName>>): Map<UUID, Set<PropertyType>> {
        return inputs.map {
            edm.getEntityType(it.key).id to
                    it.value.map{edm.getPropertyType(it)}.toSet()  // pt_id to pt
        }.toMap()
    }

    private fun getEntitySetsPerType(inputs: Map<FullQualifiedName, Set<FullQualifiedName>>): Map<UUID, Set<UUID>> {
        return inputs.keys
                .map(edm::getEntityType)
                .map(EntityType::getId)
                .map{it to edm.getEntitySetsOfType(it).map { it.id }.toSet()}
                .toMap()
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
        processors.add(processor)

        processor.getInputs()
                .map { input ->
                    // Pair<EntityType Fqn, Set<PropertyType Fqn>>
                    input.value
                            .map { edm.getPropertyTypeId(it) } // List<PropertyType UUID>
                            .map { Propagation(edm.getEntityType(input.key).id, it, processor.isSelf()) }.toSet()
                } // List<Set<Propagation>>
                .forEach {
                    val outputProp = Propagation(
                            edm.getEntityType(processor.getOutput().first).id,
                            edm.getPropertyTypeId(processor.getOutput().second),
                            processor.isSelf()
                    )
                    it.forEach { // Input propagations
                        inputProp ->
                        singleForwardPropagationGraph.getOrPut(inputProp) { mutableSetOf() }.add(outputProp)
                    }
                }
    }

    private fun tryLockEntitySet(entitySetId: UUID): Boolean {
        return processingLocks.putIfAbsent(entitySetId, System.currentTimeMillis() + EXPIRATION_MILLIS) == null
    }
}

/**
 * This will generate SQL that will tombstone existing computed property values for a propagation target
 */
internal fun buildTombstoneSql(
        neighborEntitySetIds: Collection<UUID>, //dst propagation set
        neighborPropertyTypeId: UUID, //dst propagation set
        entitySetIds: Map<UUID, Collection<UUID>>, //src propagation set // per entity type id
        propertyTypes: Map<UUID, Set<PropertyType>>, //src propagation set // per entity type id
        associationType: Boolean
): List<String> {
    val propertyTable = buildGetActivePropertiesSql(entitySetIds, propertyTypes, mapOf())
    val edgesSql = if (associationType) {
        buildFilteredEdgesSqlForAssociations(neighborEntitySetIds) // TODO add input is necessary
    } else {
        buildFilteredEdgesSqlForEntities(neighborEntitySetIds)
    }

    val propagations = edgesSql.map {
        "SELECT * " +
                "FROM ($propertyTable) as blocked_property " +
                "INNER JOIN  $it USING($entityKeyIdColumns) "
    }
    val propertyTableName = quote(DataTables.propertyTableName(neighborPropertyTypeId))
    val version = -System.currentTimeMillis()
    return propagations
            .map {
                "UPDATE $propertyTableName SET version = version, versions = versions || ARRAY[$version]" +
                        "FROM ($it) as propagations " +
                        "WHERE $propertyTableName.${ENTITY_SET_ID.name} = propagations.$TARGET_ENTITY_SET_ID " +
                        " AND  $propertyTableName.${ID_VALUE.name} = propagations.$TARGET_ENTITY_KEY_ID "
            }
}


internal fun buildComputeQueries(
        computeExpression: String,
        filterExpressions: Map<UUID, Set<ValueFilter<*>>>,
        outputEntitySetIds: Collection<UUID>,
        outputProperty: UUID,
        fqn: String,
        entitySetIdsPerType: Map<UUID, Collection<UUID>>, //Propagation
        propertyTypes: Map<UUID, Set<PropertyType>>, //Propagation
        associationType: Boolean,
        isSelf: Boolean
): List<String> {
    checkState(entitySetIdsPerType.isNotEmpty(), "Entity set ids are empty (no input entity set present)")

    val propertyTable = buildGetActivePropertiesSql(entitySetIdsPerType, propertyTypes, filterExpressions)
    val propertyTableName = quote(DataTables.propertyTableName(outputProperty))


    val outputPropertyKeys = " (SELECT " +
            "${ENTITY_SET_ID.name} as $TARGET_ENTITY_SET_ID, ${ENTITY_ID.name} as $TARGET_ENTITY_KEY_ID, " +
            "${LAST_WRITE.name}, ${LAST_PROPAGATE.name} " +
            "FROM $propertyTableName) "
    val targetEntityKeyIdColumns = "$TARGET_ENTITY_SET_ID, $TARGET_ENTITY_KEY_ID"
    val outputPropertyFqns = propertyTypes.values.flatten().map { quote(it.type.fullQualifiedNameAsString) }.joinToString(separator = ", ")


    val propagation = if(isSelf) {
        "(SELECT * " +
                "FROM (SELECT ${ENTITY_SET_ID.name} as $TARGET_ENTITY_SET_ID, ${ENTITY_ID.name} as $TARGET_ENTITY_KEY_ID, $outputPropertyFqns  " +
                "FROM ($propertyTable)) as blocked_property "
                "LEFT JOIN $outputPropertyKeys AS dirty USING($targetEntityKeyIdColumns) ) as $propertyTableName " //TODO add where 

    } else {
        val queries = buildFilteredEdgesSqlForAssociations(outputEntitySetIds) + buildFilteredEdgesSqlForEntities(outputEntitySetIds) // TODO add input entity sets
        val edgeQuery = queries.joinToString( " \nUNION\n ").map{ "$it"}

        "(SELECT * " +
                "FROM $edgeQuery " +
                "LEFT JOIN ($propertyTable) as blocked_property USING($entityKeyIdColumns)" + //TODO using????
                "LEFT JOIN $outputPropertyKeys as dirty USING($targetEntityKeyIdColumns) ) as propagations " //TODO add where last prop...

    }

    val version = System.currentTimeMillis()

    return listOf("INSERT INTO $propertyTableName ($entityKeyIdColumns,hash,$fqn,version,versions,last_propagate, last_write) " +
            "(SELECT $targetEntityKeyIdColumns,digest(($computeExpression)::text,'sha1'),$computeExpression,$version,ARRAY[$version],now(),now() " +
            "FROM $propagation " +
            "GROUP BY ( $targetEntityKeyIdColumns ) ) " +
            "ON CONFLICT (${ENTITY_SET_ID.name},${ID_VALUE.name}, ${HASH.name}) DO UPDATE SET " +
            "${VERSION.name} =  EXCLUDED.${VERSION.name}, " +
            "${VERSIONS.name} = $propertyTableName.${VERSIONS.name} || EXCLUDED.${VERSIONS.name}, " +
            "last_propagate=excluded.last_propagate, last_write=excluded.last_write")

}

/**
 * Prepares a propagation query for a single propagation.
 */
internal fun buildPropagationQueries(
        neighborEntitySetIds: Collection<UUID>,
        neighborPropertyTypeIds: Set<UUID>,
        entitySetIds: Collection<UUID>, //Propagation
        propertyTypes: Map<UUID, PropertyType>, //Propagation
        associationType: Boolean,
        isSelf: Boolean
): List<String> {
    checkState(entitySetIds.isNotEmpty(), "Entity set ids are empty (no input entity set present)")

    val propertyTable = buildGetBlockedPropertiesSql(entitySetIds, propertyTypes)

    val propagations = if(isSelf) {
        listOf("SELECT ${ENTITY_SET_ID.name} as $TARGET_ENTITY_SET_ID, ${ID_VALUE.name} as $TARGET_ENTITY_KEY_ID " +
                "FROM ($propertyTable) as blocked_property ")
    } else {
        val edgesSql = if (associationType) {
            buildFilteredEdgesSqlForAssociations(neighborEntitySetIds) // TODO add input entity sets
        } else {
            buildFilteredEdgesSqlForEntities(neighborEntitySetIds) // TODO add input entity sets if necessary
        }

        edgesSql.map {
            "SELECT * " +
                    "FROM ($propertyTable) as blocked_property " +
                    "INNER JOIN  ($it) as filtered_edges USING($entityKeyIdColumns) "
        }
    }

    return neighborPropertyTypeIds.flatMap { neighborPropertyTypeId ->
        val propertyTableName = quote(DataTables.propertyTableName(neighborPropertyTypeId))
        propagations
                .map {
                    "UPDATE $propertyTableName SET ${LAST_WRITE.name} = now() " +
                            "FROM ($it) as propagations " +
                            "WHERE $propertyTableName.${ENTITY_SET_ID.name} = propagations.$TARGET_ENTITY_SET_ID " +
                            " AND  $propertyTableName.${ID_VALUE.name} = propagations.$TARGET_ENTITY_KEY_ID "
                }
    }
}

const val TARGET_ENTITY_SET_ID = "target_entity_set_id"
const val TARGET_ENTITY_KEY_ID = "target_entity_key_Id"

//A property is marked as propagted when it completes a compute step with no unpropagated neighbors/parents.
internal fun buildGetActivePropertiesSql(
        entitySetIdsPerType: Map<UUID, Collection<UUID>>,
        propertyTypeIdsPerEntitySetType: Map<UUID, Set<PropertyType>>,
        propertyTypeFilters: Map<UUID, Set<ValueFilter<*>>>
): String {
    //Want to get property types across several entity sets of the same type.
    //This table can be used by entity sets to figure out where to insert new properties.

    //i.e if edge for entity exists and selected subset of properties can be propagate. Push them through to neighbors
    //The result of this query can be joined with edges and other

    val propertyTypeFiltersWithNullCheck = propertyTypeIdsPerEntitySetType.mapValues { propertyTypeFilters[it.key]?: setOf() }
    // TODO: for each property, where entity type is the same -> inner join, between them full outer join

    val selectPropertyTypesOfSameEntityType = entitySetIdsPerType.map {
        selectEntitySetWithCurrentVersionOfPropertyTypes(
                it.value.map { it to Optional.empty<Set<UUID>>() }.toMap(),
                propertyTypeIdsPerEntitySetType[it.key]!!.map { it.id to quote(it.type.fullQualifiedNameAsString) }.toMap(),
                propertyTypeIdsPerEntitySetType[it.key]!!.map{ it.id },
                it.value.map { it to propertyTypeIdsPerEntitySetType.keys }.toMap(),
                propertyTypeFiltersWithNullCheck,
                setOf(),
                false,
                propertyTypeIdsPerEntitySetType[it.key]!!.map{ it.id to (it.datatype == EdmPrimitiveTypeKind.Binary) }.toMap(),
                " AND last_propagate >= last_write ")
    }

    return selectPropertyTypesOfSameEntityType.joinToString(prefix = "(", postfix = ")", separator = ") FULL OUTER JOIN using($entityKeyIdColumns) (") // TODO check
}

internal fun buildGetBlockedPropertiesSql(
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
            " AND last_propagate < last_write "
    )
}


internal fun buildFilteredEdgesSqlForEntities(inputEntitySetIds:Collection<UUID>,outputEntitySetIds: Collection<UUID>): List<String> {
    val inputEntitySetsClause = inputEntitySetIds.joinToString(",") { "'$it'" }
    val outputEntitySetsClause = outputEntitySetIds.joinToString(",") { "'$it'" }
    //Only select entity sets participating in prop
    val srcEdgesSql = "SELECT ${SRC_ENTITY_SET_ID.name} as ${ENTITY_SET_ID.name}, ${SRC_ENTITY_KEY_ID.name} as ${ID_VALUE.name}, " +
            "${DST_ENTITY_SET_ID.name} as $TARGET_ENTITY_SET_ID, ${DST_ENTITY_KEY_ID.name} as $TARGET_ENTITY_KEY_ID " +
            "FROM ${EDGES.name} " +
            "WHERE ${DST_ENTITY_SET_ID.name} IN ($outputEntitySetsClause) AND ${SRC_ENTITY_SET_ID.name} IN ($inputEntitySetsClause)"

    val dstEdgesSql = "SELECT ${DST_ENTITY_SET_ID.name} as ${ENTITY_SET_ID.name}, ${DST_ENTITY_KEY_ID.name} as ${ID_VALUE.name}, " +
            "${SRC_ENTITY_SET_ID.name} as $TARGET_ENTITY_SET_ID, ${SRC_ENTITY_KEY_ID.name} as $TARGET_ENTITY_KEY_ID " +
            "FROM ${EDGES.name} " +
            "WHERE ${SRC_ENTITY_SET_ID.name} IN ($outputEntitySetsClause) AND ${DST_ENTITY_SET_ID.name} IN ($inputEntitySetsClause) "

    return listOf(srcEdgesSql, dstEdgesSql)

}

internal fun buildFilteredEdgesSqlForAssociations(inputEntitySetIds: Collection<UUID>,outputEntitySetIds: Collection<UUID>): List<String> {
    val inputEntitySetsClause = inputEntitySetIds.joinToString(",") { "'$it'" }
    val outputEntitySetsClause = outputEntitySetIds.joinToString(",") { "'$it'" }
    val edgeSrcEdgesSql = "SELECT ${EDGE_ENTITY_SET_ID.name} as ${ENTITY_SET_ID.name}, ${EDGE_ENTITY_KEY_ID.name} as ${ID_VALUE.name}, " +
            "${SRC_ENTITY_SET_ID.name} as $TARGET_ENTITY_SET_ID, ${SRC_ENTITY_KEY_ID.name} as $TARGET_ENTITY_KEY_ID " +
            "FROM ${EDGES.name} " +
            "WHERE ${EDGE_ENTITY_SET_ID.name} IN ($inputEntitySetsClause) AND ${SRC_ENTITY_SET_ID.name} IN ($outputEntitySetsClause)"

    val edgeDstEdgesSql = "SELECT ${EDGE_ENTITY_SET_ID.name} as ${ENTITY_SET_ID.name}, ${EDGE_ENTITY_KEY_ID.name} as ${ID_VALUE.name}, " +
            "${DST_ENTITY_SET_ID.name} as $TARGET_ENTITY_SET_ID, ${DST_ENTITY_KEY_ID.name} as $TARGET_ENTITY_KEY_ID " +
            "FROM ${EDGES.name} " +
            "WHERE ${EDGE_ENTITY_SET_ID.name} IN ($inputEntitySetsClause) AND ${DST_ENTITY_SET_ID.name} IN ($outputEntitySetsClause)"

    return listOf(edgeSrcEdgesSql, edgeDstEdgesSql)
}