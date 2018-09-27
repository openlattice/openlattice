package com.openlattice.graph.processing

import com.google.common.base.Preconditions.checkState
import com.google.common.base.Stopwatch
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.core.IMap
import com.hazelcast.query.QueryConstants
import com.openlattice.analysis.requests.Filter
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
            while(compute() > 0) {
            }
            while (propagate() > 0) {
            }
        }
    }

    private fun propagate(): Int {
        //Basically update all neighbors of where last_received > last_propagated. No filtering required because we
        //want to continuously be propagating the signal of this shouldn't be used for computation.

//        val pTypes = propertyTypes.values.map{ it.id to it }.toMap()
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
            val entitySetIds = getEntitySets(processor.getInputs())
            val propertyTypes = getPropertyTypes(processor.getInputs())
            val outputEntitySetIds = getEntitySets(mapOf(processor.getOutputs()).mapValues { setOf(it.value) })
            val outputPropertyType = getPropertyTypes(mapOf(processor.getOutputs()).mapValues { setOf(it.value) })
            val associationType = edm.getAssociationTypeSafe(edm.getEntityType(processor.getOutputs().first).id) != null
            val filters = processor.getFilters().values.flatMap { it.map{ edm.getPropertyTypeId(it.key) to setOf(it.value)} }.toMap()
            try{
                val computeQueries = buildComputeQueries(
                        processor.getSql(),
                        filters,
                        outputEntitySetIds,
                        outputPropertyType.keys.first(),
                        quote(outputPropertyType.values.first().type.fullQualifiedNameAsString),
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
                logger.error("Couldn't compute property type ${processor.getOutputs().second} of entity type ${processor.getOutputs().first}: $e")
                 0
            }
        }.sum()

        return count
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
                            edm.getEntityType(processor.getOutputs().first).id,
                            edm.getPropertyTypeId(processor.getOutputs().second),
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
        entitySetIds: Collection<UUID>, //src propagation set
        propertyTypes: Map<UUID, PropertyType>, //src propagation set
        associationType: Boolean
): List<String> {
    val propertyTable = buildGetActivePropertiesSql(entitySetIds, propertyTypes, mapOf())
    val edgesSql = if (associationType) {
        buildFilteredEdgesSqlForAssociations(neighborEntitySetIds)
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
        neighborEntitySetIds: Collection<UUID>,
        neighborPropertyTypeId: UUID,
        fqn: String,
        entitySetIds: Collection<UUID>, //Propagation
        propertyTypes: Map<UUID, PropertyType>, //Propagation
        associationType: Boolean,
        isSelf: Boolean
): List<String> {
    checkState(entitySetIds.isNotEmpty(), "Entity set ids are empty (no output entity set present)")

    val propertyTable = buildGetActivePropertiesSql(entitySetIds, propertyTypes, filterExpressions)
    val propertyTableName = quote(DataTables.propertyTableName(neighborPropertyTypeId))

    val propagations = if(isSelf) {
        listOf("(SELECT * " +
                "FROM ($propertyTable) as blocked_property ) as $propertyTableName ")

    } else {
        val edgesSql = if (associationType) {
            buildFilteredEdgesSqlForAssociations(neighborEntitySetIds)
        } else {
            buildFilteredEdgesSqlForEntities(neighborEntitySetIds)
        }

        edgesSql.map {
            "(SELECT * " +
                    "FROM ($propertyTable) as blocked_property " +
                    "INNER JOIN  ($it) as filtered_edges USING($entityKeyIdColumns) ) as propagations " +
                    "INNER JOIN $propertyTableName ON ($propertyTableName.${ENTITY_SET_ID.name} = propagations.$TARGET_ENTITY_SET_ID AND  $propertyTableName.${ID_VALUE.name} = propagations.$TARGET_ENTITY_KEY_ID) "
        }
    }

    val version = System.currentTimeMillis()
    val propertyTableEntityKeyIdColumns = listOf(ENTITY_SET_ID.name, ID_VALUE.name).joinToString(prefix = "$propertyTableName.", separator = ", $propertyTableName.")

    return propagations
            .map {
                "INSERT INTO $propertyTableName ($entityKeyIdColumns,hash,$fqn,version,versions,last_propagate, last_write) " +
                        "(SELECT $propertyTableEntityKeyIdColumns,digest(($computeExpression)::text,'sha1'),$computeExpression,$version,ARRAY[$version],now(),now() " +
                        "FROM $it " +
                        "GROUP BY ( $propertyTableEntityKeyIdColumns ) ) " +
                        "ON CONFLICT (${ENTITY_SET_ID.name},${ID_VALUE.name}, ${HASH.name}) DO UPDATE SET " +
                        "${VERSION.name} =  EXCLUDED.${VERSION.name}, " +
                        "${VERSIONS.name} = $propertyTableName.${VERSIONS.name} || EXCLUDED.${VERSIONS.name}, " +
                        "last_propagate=excluded.last_propagate, last_write=excluded.last_write"
            }
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
    checkState(entitySetIds.isNotEmpty(), "Entity set ids are empty (no output entity set present)")

    val propertyTable = buildGetBlockedPropertiesSql(entitySetIds, propertyTypes)

    val propagations = if(isSelf) {
        listOf("SELECT ${ENTITY_SET_ID.name} as $TARGET_ENTITY_SET_ID, ${ID_VALUE.name} as $TARGET_ENTITY_KEY_ID " +
                "FROM ($propertyTable) as blocked_property ")
    } else {
        val edgesSql = if (associationType) {
            buildFilteredEdgesSqlForAssociations(neighborEntitySetIds)
        } else {
            buildFilteredEdgesSqlForEntities(neighborEntitySetIds)
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
        entitySetIds: Collection<UUID>, propertyTypeIds: Map<UUID, PropertyType>, propertyTypeFilters: Map<UUID, Set<ValueFilter<*>>>
): String {
    //Want to get property types across several entity sets of the same type.
    //This table can be used by entity sets to figure out where to insert new properties.

    //i.e if edge for entity exists and selected subset of properties can be propagate. Push them through to neighbors
    //The result of this query can be joined with edges and other

    val propertyTypeFiltersWithNullCheck = propertyTypeIds.mapValues { propertyTypeFilters[it.key]?: setOf() }

    return selectEntitySetWithCurrentVersionOfPropertyTypes(
            entitySetIds.map { it to Optional.empty<Set<UUID>>() }.toMap(),
            propertyTypeIds.mapValues { quote(it.value.type.fullQualifiedNameAsString) },
            propertyTypeIds.keys,
            entitySetIds.map { it to propertyTypeIds.keys }.toMap(),
            propertyTypeFiltersWithNullCheck,
            setOf(),
            false,
            propertyTypeIds.mapValues { it.value.datatype == EdmPrimitiveTypeKind.Binary },
            " AND last_propagate >= last_write "
    )
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


internal fun buildFilteredEdgesSqlForEntities(entitySetIds: Collection<UUID>): List<String> {
    val entitySetsClause = entitySetIds.joinToString(",") { "'$it'" }
    //Only select entity sets participating in prop
    val srcEdgesSql = "SELECT ${SRC_ENTITY_SET_ID.name} as ${ENTITY_SET_ID.name}, ${SRC_ENTITY_KEY_ID.name} as ${ID_VALUE.name}, " +
            "${DST_ENTITY_SET_ID.name} as $TARGET_ENTITY_SET_ID, ${DST_ENTITY_KEY_ID.name} as $TARGET_ENTITY_KEY_ID " +
            "FROM ${EDGES.name} WHERE ${DST_ENTITY_SET_ID.name} IN ($entitySetsClause)"

    val dstEdgesSql = "SELECT ${DST_ENTITY_SET_ID.name} as ${ENTITY_SET_ID.name}, ${DST_ENTITY_KEY_ID.name} as ${ID_VALUE.name}, " +
            "${SRC_ENTITY_SET_ID.name} as $TARGET_ENTITY_SET_ID, ${SRC_ENTITY_KEY_ID.name} as $TARGET_ENTITY_KEY_ID " +
            "FROM ${EDGES.name} WHERE ${DST_ENTITY_SET_ID.name} IN ($entitySetsClause)"

    return listOf(srcEdgesSql, dstEdgesSql)

}

internal fun buildFilteredEdgesSqlForAssociations(entitySetIds: Collection<UUID>): List<String> {
    val entitySetsClause = entitySetIds.joinToString(",") { "'$it'" }
    val edgeSrcEdgesSql = "SELECT ${EDGE_ENTITY_SET_ID.name} as ${ENTITY_SET_ID.name}, ${EDGE_ENTITY_KEY_ID.name} as ${ID_VALUE.name}, " +
            "${SRC_ENTITY_SET_ID.name} as $TARGET_ENTITY_SET_ID, ${SRC_ENTITY_KEY_ID.name} as $TARGET_ENTITY_KEY_ID " +
            "FROM ${EDGES.name} WHERE ${SRC_ENTITY_SET_ID.name} IN ($entitySetsClause)"

    val edgeDstEdgesSql = "SELECT ${EDGE_ENTITY_SET_ID.name} as ${ENTITY_SET_ID.name}, ${EDGE_ENTITY_KEY_ID.name} as ${ID_VALUE.name}, " +
            "${DST_ENTITY_SET_ID.name} as $TARGET_ENTITY_SET_ID, ${DST_ENTITY_KEY_ID.name} as $TARGET_ENTITY_KEY_ID " +
            "FROM ${EDGES.name} WHERE ${SRC_ENTITY_SET_ID.name} IN ($entitySetsClause)"

    return listOf(edgeSrcEdgesSql, edgeDstEdgesSql)
}