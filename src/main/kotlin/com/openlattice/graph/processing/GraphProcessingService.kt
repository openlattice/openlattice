package com.openlattice.graph.processing

import com.google.common.base.Preconditions.checkState
import com.google.common.base.Stopwatch
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.core.IMap
import com.hazelcast.query.QueryConstants
import com.openlattice.analysis.requests.ValueFilter
import com.openlattice.data.storage.entityKeyIdColumns
import com.openlattice.data.storage.selectEntitySetWithCurrentVersionOfPropertyTypes
import com.openlattice.datastore.services.EdmManager
import com.openlattice.edm.EntitySet
import com.openlattice.edm.type.EntityType
import com.openlattice.edm.type.PropertyType
import com.openlattice.graph.processing.processors.AssociationProcessor
import com.openlattice.graph.processing.processors.GraphProcessor
import com.openlattice.graph.processing.processors.SelfProcessor
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.postgres.DataTables
import com.openlattice.postgres.DataTables.LAST_WRITE
import com.openlattice.postgres.DataTables.quote
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresTable.EDGES
import com.zaxxer.hikari.HikariDataSource
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.LoggerFactory
import java.lang.IllegalArgumentException
import java.lang.IllegalStateException
import java.sql.SQLException
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock

class GraphProcessingService(
        private val edm: EdmManager,
        private val hds: HikariDataSource,
        hazelcastInstance: HazelcastInstance,
        processorsToRegister: Set<GraphProcessor>
) {
    private val propertyTypes: IMap<UUID, PropertyType> = hazelcastInstance.getMap(HazelcastMap.PROPERTY_TYPES.name)
    private val processors = mutableSetOf<GraphProcessor>()
    private val processingLocks: IMap<UUID, Long> = hazelcastInstance.getMap(
            HazelcastMap.INDEXING_GRAPH_PROCESSING.name
    )

    private val propagationGraphProcessor = PropagationGraphProcessor(edm)

    companion object {
        private val logger = LoggerFactory.getLogger(GraphProcessingService::class.java)
    }

    init {
        processorsToRegister.forEach { register(it) }
        if(propagationGraphProcessor.hasCycle()) {
            throw IllegalStateException("There is a cycle in the background graph processing")
        }
        processingLocks.addIndex(QueryConstants.THIS_ATTRIBUTE_NAME.value(), true)
    }

    private val taskLock = ReentrantLock()

    fun step() {
        if (taskLock.tryLock()) {
            markRootsPropagated()
            while(compute() > 0){}
            propagate()

            taskLock.unlock()
        }
    }

    private fun markRootsPropagated(): Int {
        val count = propagationGraphProcessor.rootInputPropagations.map {
            markPropagated(it)
        }.sum()

        logger.info("Marked $count root entities as propagated")
        return count
    }

    private fun markPropagated(rootProp: Propagation): Int {
        val entitySetIds = edm.getEntitySetsOfType(rootProp.entityTypeId).map { it.id }
        val propertyTypes = this.propertyTypes.getAll(setOf(rootProp.propertyTypeId))

        try {
            checkState(entitySetIds.isNotEmpty(), "Entity set ids are empty (no input entity set present)")
            val propQuery = buildMarkPropagatedQuery(entitySetIds, propertyTypes)

            return hds.connection.use {
                it.use {
                    try {
                        it.createStatement().use {
                            it.executeUpdate(propQuery)
                        }
                    } catch(e:SQLException) {
                        logger.error("Unable to mark root property as propagated with sql query: {} ${System.lineSeparator()}$e", propQuery)
                        0
                    }
                }
            }
        } catch(e: IllegalStateException) {
            logger.error("Couldn't mark root entities as propagated ${edm.getEntityTypeFqn(rootProp.entityTypeId)}: $e")
            return 0
        }
    }

    private fun propagate(): Int {
        //Basically update all neighbors of where last_write > last_propagated. No filtering required because we
        //want to continuously be propagating the signal of this

        var count = Int.MAX_VALUE
        while (count > 0) {
            val w = Stopwatch.createStarted()
            count =
                    propagationGraphProcessor.singleForwardPropagationGraph.map {
                        markIfNeedsPropagation(it.key, it.value, false)
                    }.sum() +
                    propagationGraphProcessor.selfPropagationGraph.map {
                        markIfNeedsPropagation(it.key, it.value, true)
                    }.sum()
            logger.info("Propagated $count in ${w.elapsed(TimeUnit.MILLISECONDS)}ms")
        }
        return count
    }

    private fun markIfNeedsPropagation(input: Propagation, outputs: Set<Propagation>, isSelf: Boolean): Int {
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
                    isSelf
            )

            return hds.connection.use { conn ->
                conn.autoCommit = false
                try {
                    conn.createStatement().use { stmt ->
                        return queries.map(stmt::executeUpdate).sum()
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
            return 0
        }
    }

    private fun compute(): Int {
        val count = processors.map { processor ->
            val outputPropertyType = getPropertyTypes(mapOf(processor.getOutput()).mapValues { setOf(it.value) }).values.first()
            val filters = processor.getFilters().values.flatMap { it.map{ edm.getPropertyTypeId(it.key) to setOf(it.value)} }.toMap()

            logger.info("Starting computing aggregated values for property type ${outputPropertyType.type.fullQualifiedNameAsString}")

            try{
                val computeQuery = when(processor) {
                    is AssociationProcessor -> {
                        val srcEntitySetId = getEntitySets(processor.getSrcInputs())
                        val srcPropertyTypes = getPropertyTypes(processor.getSrcInputs())
                        val srcAliases = processor.getSrcInputAliases()
                        val edgeEntitySetId = getEntitySets(processor.getEdgeInputs())
                        val edgePropertyTypes = getPropertyTypes(processor.getEdgeInputs())
                        val edgeAliases = processor.getEdgeInputAliases()
                        val dstEntitySetId = getEntitySets(processor.getDstInputs())
                        val dstPropertyTypes = getPropertyTypes(processor.getDstInputs())
                        val dstAliases = processor.getDstInputAliases()

                        val targetEntityKeyIdColumns = when {
                            processor.getSrcInputs().keys.contains(processor.getOutput().first) -> Pair(SRC_ENTITY_SET_ID.name, SRC_ENTITY_KEY_ID.name)
                            processor.getDstInputs().keys.contains(processor.getOutput().first) -> Pair(DST_ENTITY_SET_ID.name, DST_ENTITY_KEY_ID.name)
                            else -> Pair(EDGE_ENTITY_SET_ID.name, EDGE_ENTITY_KEY_ID.name)
                        }

                        buildComputeQueriesForAssociation(
                                processor.getSql(),
                                filters,
                                outputPropertyType.id,
                                quote(outputPropertyType.type.fullQualifiedNameAsString),
                                srcEntitySetId,
                                srcPropertyTypes,
                                srcAliases,
                                edgeEntitySetId,
                                edgePropertyTypes,
                                edgeAliases,
                                dstEntitySetId,
                                dstPropertyTypes,
                                dstAliases,
                                targetEntityKeyIdColumns)
                    }
                    is SelfProcessor -> {
                        val entitySetIds = getEntitySets(processor.getInputs())
                        val propertyTypes = getPropertyTypes(processor.getInputs())

                         buildComputeQueriesForSelf(
                                processor.getSql(),
                                filters,
                                outputPropertyType.id,
                                quote(outputPropertyType.type.fullQualifiedNameAsString),
                                entitySetIds,
                                propertyTypes)
                    }
                    else -> throw IllegalArgumentException("Not feasible processor")
                }


                hds.connection.use { conn ->
                    conn.use {
                        try {
                            conn.createStatement().use { stmt ->
                                val insertCount = stmt.executeUpdate(computeQuery)
                                logger.info("Finished computing $insertCount entities for property type ${outputPropertyType.type.fullQualifiedNameAsString}")

                                insertCount
                            }
                        } catch (e: SQLException) {
                            logger.error("Unable to compute aggregated values with queries: {} ${System.lineSeparator()}$e", computeQuery)
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


    private fun getPropertyTypes(inputs: Map<FullQualifiedName, Set<FullQualifiedName>>): Map<UUID, PropertyType> {
        return inputs.values.flatten().map {
            val pt = edm.getPropertyType(it)
            pt.id to pt
        }.toMap()
    }

    private fun getEntitySets(inputs: Map<FullQualifiedName, Set<FullQualifiedName>>): Set<UUID> {
        return inputs.keys
                .map(edm::getEntityType)
                .map(EntityType::getId)
                .flatMap(edm::getEntitySetsOfType)
                .map(EntitySet::getId)
                .toSet()
    }

    private fun register(processor: GraphProcessor) {
        processors.add(processor)
        propagationGraphProcessor.register(processor)
    }
}

/**
 * This will generate SQL that will tombstone existing computed property values for a propagation target
 */
internal fun buildTombstoneSql(
        neighborEntitySetIds: Collection<UUID>, //dst propagation set
        neighborPropertyTypeId: UUID, //dst propagation set
        entitySetIdsPerType:Collection<UUID>, //src propagation set // per entity type id
        propertyTypes: Map<UUID, PropertyType>, //src propagation set // per entity type id
        associationType: Boolean
): List<String> {
    val propertyTable = buildGetActivePropertiesSql(entitySetIdsPerType, propertyTypes, mapOf(), mapOf())
    val edgesSql = if (associationType) {
        buildFilteredEdgesSqlForAssociations(entitySetIdsPerType, neighborEntitySetIds) // TODO add input is necessary
    } else {
        buildFilteredEdgesSqlForEntities(entitySetIdsPerType, neighborEntitySetIds)
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


internal fun buildComputeQueriesForAssociation(
        computeExpression: String,
        filterExpressions: Map<UUID, Set<ValueFilter<*>>>,
        outputProperty: UUID,
        fqn: String,
        srcEntitySetId: Collection<UUID>, //Propagation
        srcPropertyTypes: Map<UUID, PropertyType>, //Propagation
        srcAliases: Map<FullQualifiedName, String>,
        edgeEntitySetId: Collection<UUID>, //Propagation
        edgePropertyTypes: Map<UUID, PropertyType>, //Propagation
        edgeAliases: Map<FullQualifiedName, String>,
        dstEntitySetId: Collection<UUID>, //Propagation
        dstPropertyTypes: Map<UUID, PropertyType>, //Propagation
        dstAliases: Map<FullQualifiedName, String>,
        targetEntityKeyIdColumns: Pair<String, String>
): String {
    checkState(!(srcEntitySetId.isEmpty() && dstEntitySetId.isEmpty() && edgeEntitySetId.isEmpty()), "Entity set ids are empty (no input entity set present)")

    val srcPropertyTable = buildGetActivePropertiesSql(srcEntitySetId, srcPropertyTypes, srcAliases, filterExpressions)
    val edgePropertyTable = buildGetActivePropertiesSql(edgeEntitySetId, edgePropertyTypes, edgeAliases, filterExpressions)
    val dstPropertyTable = buildGetActivePropertiesSql(dstEntitySetId, dstPropertyTypes, dstAliases, filterExpressions)

    val propertyTableName = quote(DataTables.propertyTableName(outputProperty))

    val updatableJoin = buildQueryForTargetPropertyDirtyCheck(propertyTableName, targetEntityKeyIdColumns, false)

    val edgeQuery = buildFilteredEdges(srcEntitySetId, edgeEntitySetId, dstEntitySetId)

    val propagation =
        "(SELECT * FROM($edgeQuery) as filtered_edges " +
                "INNER JOIN ($srcPropertyTable) as src_properties ON(${SRC_ENTITY_SET_ID.name} = src_properties.${ENTITY_SET_ID.name} AND ${SRC_ENTITY_KEY_ID.name} = src_properties.${ID_VALUE.name}) " +
                "INNER JOIN ($edgePropertyTable) as edge_properties ON(${EDGE_ENTITY_SET_ID.name} = edge_properties.${ENTITY_SET_ID.name} AND ${EDGE_ENTITY_KEY_ID.name} = edge_properties.${ID_VALUE.name}) " +
                "INNER JOIN ($dstPropertyTable) as dst_properties ON(${DST_ENTITY_SET_ID.name} = dst_properties.${ENTITY_SET_ID.name} AND ${DST_ENTITY_KEY_ID.name} = dst_properties.${ID_VALUE.name}) " +
                "$updatableJoin ) as propagations "

    val version = System.currentTimeMillis()

    return "INSERT INTO $propertyTableName ($entityKeyIdColumns,hash,$fqn,version,versions,last_propagate, last_write) " +
            "(SELECT ${targetEntityKeyIdColumns.first}, ${targetEntityKeyIdColumns.second},digest(($computeExpression)::text,'sha1'),$computeExpression,$version,ARRAY[$version],now(),now() " +
            "FROM $propagation " +
            "GROUP BY ( ${targetEntityKeyIdColumns.first}, ${targetEntityKeyIdColumns.second} ) ) " +
            "ON CONFLICT (${ENTITY_SET_ID.name},${ID_VALUE.name}, ${HASH.name}) DO UPDATE SET " +
            "${VERSION.name} =  EXCLUDED.${VERSION.name}, " +
            "${VERSIONS.name} = $propertyTableName.${VERSIONS.name} || EXCLUDED.${VERSIONS.name}, " +
            "last_propagate=excluded.last_propagate, last_write=excluded.last_write"

}

internal fun buildComputeQueriesForSelf(
        computeExpression: String,
        filterExpressions: Map<UUID, Set<ValueFilter<*>>>,
        outputProperty: UUID,
        fqn: String,
        entitySetIds: Collection<UUID>, //Propagation
        propertyTypes: Map<UUID, PropertyType> //Propagation
): String {
    checkState(entitySetIds.isNotEmpty(), "Entity set ids are empty (no input entity set present)")

    val propertyTable = buildGetActivePropertiesSql(entitySetIds, propertyTypes, mapOf(), filterExpressions)
    val propertyTableName = quote(DataTables.propertyTableName(outputProperty))

    val outputPropertyFqns = propertyTypes.values.map { quote(it.type.fullQualifiedNameAsString) }.joinToString(separator = ", ")

    val updatableJoin = buildQueryForTargetPropertyDirtyCheck(propertyTableName, Pair(ENTITY_SET_ID.name, ID_VALUE.name))

    val propagation =
        "(SELECT * FROM (SELECT * " +
                "FROM (SELECT $entityKeyIdColumns, $outputPropertyFqns  " +
                "FROM ($propertyTable) as joined_properties ) as blocked_properties " +
                "$updatableJoin ) as updatable) as propagations "

    val version = System.currentTimeMillis()

    return "INSERT INTO $propertyTableName ($entityKeyIdColumns,hash,$fqn,version,versions,last_propagate, last_write) " +
            "(SELECT $entityKeyIdColumns,digest(($computeExpression)::text,'sha1'),$computeExpression,$version,ARRAY[$version],now(),now() " +
            "FROM $propagation " +
            "GROUP BY ( $entityKeyIdColumns ) ) " +
            "ON CONFLICT (${ENTITY_SET_ID.name},${ID_VALUE.name}, ${HASH.name}) DO UPDATE SET " +
            "${VERSION.name} =  EXCLUDED.${VERSION.name}, " +
            "${VERSIONS.name} = $propertyTableName.${VERSIONS.name} || EXCLUDED.${VERSIONS.name}, " +
            "last_propagate=excluded.last_propagate, last_write=excluded.last_write"
}


internal fun buildQueryForTargetPropertyDirtyCheck(propertyTableName: String, joinColumns: Pair<String, String>, joinSameColumnNames:Boolean = true): String {
    val outputPropertyKeys = " (SELECT " +
            "${ENTITY_SET_ID.name}, ${ID_VALUE.name}, " +
            "${LAST_WRITE.name}, ${LAST_PROPAGATE.name} " +
            "FROM $propertyTableName) "

    val joinClause = if(joinSameColumnNames)  {
        "USING(${joinColumns.first}, ${joinColumns.second})"
    } else {
        "ON(${joinColumns.first} = dirty.${ENTITY_SET_ID.name} AND ${joinColumns.second} = dirty.${ID_VALUE.name})"
    }
    return "LEFT JOIN $outputPropertyKeys AS dirty $joinClause " +
    "WHERE (${LAST_WRITE.name} is NULL AND ${LAST_PROPAGATE.name} is NULL) OR  (${LAST_WRITE.name} > ${LAST_PROPAGATE.name})"

}

/**
 * Prepares a propagation query for a single propagation.
 */
internal fun buildPropagationQueries(
        outputEntitySetIds: Collection<UUID>,
        outputPropertyTypeIds: Set<UUID>,
        entitySetIds: Collection<UUID>, //Propagation
        propertyTypes: Map<UUID, PropertyType>, //Propagation
        associationType: Boolean,
        isSelf: Boolean
): List<String> {
    checkState(entitySetIds.isNotEmpty(), "Entity set ids are empty (no input entity set present)")

    val filters = propertyTypes.keys.map { it to setOf<ValueFilter<*>>() }.toMap()
    val propertyTable = buildGetBlockedPropertiesSql(entitySetIds, propertyTypes, filters)

    val propagations = if(isSelf) {
        listOf("SELECT ${ENTITY_SET_ID.name} as $TARGET_ENTITY_SET_ID, ${ID_VALUE.name} as $TARGET_ENTITY_KEY_ID " +
                "FROM ($propertyTable) as blocked_property ")
    } else {
        val edgesSql = if (associationType) {
            buildFilteredEdgesSqlForAssociations(entitySetIds, outputEntitySetIds)
        } else {
            buildFilteredEdgesSqlForEntities(entitySetIds, outputEntitySetIds)
        }

        edgesSql.map {
            "SELECT * " +
                    "FROM ($propertyTable) as blocked_property " +
                    "INNER JOIN  ($it) as filtered_edges USING($entityKeyIdColumns) "
        }
    }

    return outputPropertyTypeIds.flatMap { outputPropertyTypeId ->
        val propertyTableName = quote(DataTables.propertyTableName(outputPropertyTypeId))
        propagations
                .map {
                    "UPDATE $propertyTableName SET ${LAST_WRITE.name} = now() " +
                            "FROM ($it) as propagations " +
                            "WHERE $propertyTableName.${ENTITY_SET_ID.name} = propagations.$TARGET_ENTITY_SET_ID " +
                            " AND  $propertyTableName.${ID_VALUE.name} = propagations.$TARGET_ENTITY_KEY_ID "
                }
    }
}

internal fun buildMarkPropagatedQuery (
        entitySetIds: Collection<UUID>, //Propagation
        propertyTypes: Map<UUID, PropertyType> //Propagation
): String {

    val propertyTableName = quote(DataTables.propertyTableName(propertyTypes.values.first().id))
    val entitySetsClause = entitySetIds.joinToString(",") { "'$it'" }
    return "UPDATE $propertyTableName SET ${LAST_PROPAGATE.name} = now() " +
            "WHERE  ${ENTITY_SET_ID.name} IN ( $entitySetsClause )"
}




const val TARGET_ENTITY_SET_ID = "target_entity_set_id"
const val TARGET_ENTITY_KEY_ID = "target_entity_key_Id"


internal fun buildGetActivePropertiesSql(
        entitySetIds: Collection<UUID>,
        propertyTypes: Map<UUID, PropertyType>,
        propertyAliases: Map<FullQualifiedName, String>,
        propertyTypeFilters: Map<UUID, Set<ValueFilter<*>>>): String {

    val propertyTypeFiltersWithNullCheck = propertyTypes.mapValues { propertyTypeFilters[it.key]?: setOf() }
    val columns = if(propertyAliases.isEmpty()) {
        "*"
    } else {
        listOf(ENTITY_SET_ID.name, ID_VALUE.name)
                .union(propertyTypes.map {
                    val alias = propertyAliases[it.value.type] ?: it.value.type.fullQualifiedNameAsString
                    "${DataTables.quote(it.value.type.fullQualifiedNameAsString)} AS ${DataTables.quote(alias)}"
                })
                .joinToString(separator = ", ")
    }

    val selectEntities = selectEntitySetWithCurrentVersionOfPropertyTypes(
            entitySetIds.map { it to Optional.empty<Set<UUID>>() }.toMap(),
            propertyTypes.mapValues { quote(it.value.type.fullQualifiedNameAsString) }.toMap(),
            propertyTypes.keys,
            entitySetIds.map { it to propertyTypes.keys }.toMap(),
            propertyTypeFiltersWithNullCheck,
            setOf(),
            false,
            propertyTypes.mapValues{ it.value.datatype == EdmPrimitiveTypeKind.Binary }.toMap(),
            " AND last_propagate >= last_write ")

    return " SELECT $columns FROM ($selectEntities) as entities "
}

internal fun buildGetBlockedPropertiesSql(
        entitySetIds: Collection<UUID>,
        propertyTypeIds: Map<UUID, PropertyType>,
        propertyTypeFilters: Map<UUID, Set<ValueFilter<*>>>
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
            propertyTypeFilters,
            setOf(),
            false,
            propertyTypeIds.mapValues { it.value.datatype == EdmPrimitiveTypeKind.Binary },
            " AND last_propagate < last_write "
    )
}

internal fun buildFilteredEdges(srcEntitySetIds: Collection<UUID>, edgeEntitySetIds: Collection<UUID>, dstEntitySetIds: Collection<UUID>): String {
    val srcEntitySetClause = srcEntitySetIds.joinToString(",") { "'$it'" }
    val edgeEntitySetClause = edgeEntitySetIds.joinToString(",") { "'$it'" }
    val dstEntitySetIdClause = dstEntitySetIds.joinToString(",") { "'$it'" }

    val srcFilter = when(srcEntitySetClause) {
        "" -> "TRUE AND"
        else -> "${SRC_ENTITY_SET_ID.name} IN ($srcEntitySetClause) AND "
    }
    val edgeFilter = when(edgeEntitySetClause) {
        "" -> "TRUE AND"
        else -> "${EDGE_ENTITY_SET_ID.name} IN ($edgeEntitySetClause) AND "
    }
    val dstFilter = when(dstEntitySetIdClause) {
        "" -> "TRUE"
        else -> "${DST_ENTITY_SET_ID.name} IN ($dstEntitySetIdClause) "
    }

    return "SELECT ${SRC_ENTITY_SET_ID.name}, ${SRC_ENTITY_KEY_ID.name}, " +
            "${EDGE_ENTITY_SET_ID.name}, ${EDGE_ENTITY_KEY_ID.name}, " +
            "${DST_ENTITY_SET_ID.name}, ${DST_ENTITY_KEY_ID.name} " +
            "FROM ${EDGES.name} WHERE $srcFilter $edgeFilter $dstFilter"
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