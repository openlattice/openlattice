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

import com.fasterxml.jackson.databind.ObjectMapper
import com.openlattice.analysis.requests.Filter
import com.openlattice.authorization.AuthorizationManager
import com.openlattice.data.DataEdgeKey
import com.openlattice.data.EntityDataKey
import com.openlattice.data.Property
import com.openlattice.data.storage.ByteBlobDataManager
import com.openlattice.data.storage.MetadataOption
import com.openlattice.data.storage.PostgresEntityDataQueryService
import com.openlattice.data.storage.selectEntitySetWithCurrentVersionOfPropertyTypes
import com.openlattice.datastore.services.EdmManager
import com.openlattice.edm.EntitySet
import com.openlattice.edm.type.PropertyType
import com.openlattice.postgres.DataTables.quote
import com.openlattice.postgres.PostgresArrays
import com.openlattice.postgres.PostgresColumn
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresTable
import com.openlattice.postgres.PostgresTable.EDGES
import com.openlattice.postgres.PostgresTable.GRAPH_QUERIES
import com.openlattice.postgres.streams.PostgresIterable
import com.openlattice.postgres.streams.StatementHolder
import com.zaxxer.hikari.HikariDataSource
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind
import java.sql.Connection
import java.sql.Statement
import java.util.*
import java.util.function.Function
import java.util.function.Supplier

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class PostgresGraphQueryService(
        private val hds: HikariDataSource,
        private val edm: EdmManager,
        private val pgDataService: PostgresEntityDataQueryService,
        private val authorizationManager: AuthorizationManager,
        private val byteBlobDataManager: ByteBlobDataManager,
        private val mapper: ObjectMapper
) : GraphQueryService {
    override fun getEntitySetForIds(ids: Set<UUID>): Map<UUID, UUID> {
        return PostgresIterable<Pair<UUID, UUID>>(
                Supplier {
                    val connection = hds.connection
                    val ps = connection.prepareStatement(
                            "SELECT ${PostgresColumn.ENTITY_SET_ID.name},${ID_VALUE.name} FROM ${PostgresTable.IDS.name} WHERE ${ID_VALUE.name} = ANY(?)"
                    )
                    val arr = PostgresArrays.createUuidArray(connection, ids)
                    ps.setArray(1, arr)

                    StatementHolder(connection, ps, ps.executeQuery())
                },
                Function {
                    return@Function it.getObject(PostgresColumn.ID_VALUE.name, UUID::class.java) to
                            it.getObject(PostgresColumn.ENTITY_SET_ID.name, UUID::class.java)
                }
        ).toMap()
    }

    override fun submitQuery(
            query: NeighborhoodQuery,
            propertyTypes: Map<UUID, PropertyType>,
            authorizedPropertyTypesByEntitySet: Map<UUID, Map<UUID, PropertyType>>
    ): Neighborhood {
        /*
         * While it would be more efficient to group by entity set type and query all at once, filters can vary
         * by element so instead we
         *
         * (1) Create temporary view for each neighbor selection and filter selection.
         * (2) Repeatedly inner join filter tables with view for neighbor selection to build final (src|dst, edge) neighborhood selection.
         * (3)
         * (4) Future to allow AND of neighbor selections just inner join each set of intermediate neighborhood selections to generate final neighbor selection.
         *
         *
         */

        val authorizedPropertyTypes = authorizedPropertyTypesByEntitySet.mapValues { it.value.keys }
        val ids = query.ids

        val entitiesWithoutData = mutableMapOf<UUID, MutableSet<UUID>>()
        val entities = mutableMapOf<UUID, MutableMap<UUID, MutableMap<UUID, List<Property>>>>()
        val associations = mutableMapOf<UUID, MutableMap<UUID, MutableMap<UUID, NeighborIds>>>()
        val neighborhood = Neighborhood(ids, entities, associations)
        val propertyTypeFqns = propertyTypes.mapValues { quote(it.value.type.fullQualifiedNameAsString) }
        query.srcSelections.forEachIndexed { index, selection ->
            val entityFilterDefinitions = getFilterDefinitions(
                    selection.entityTypeIds,
                    selection.entitySetIds,
                    selection.entityFilters
            )

            val associationFilterDefinitions = getFilterDefinitions(
                    selection.associationTypeIds,
                    selection.associationEntitySetIds,
                    selection.associationFilters
            )
            val entitySetIds = entityFilterDefinitions.flatMap { it.entitySetIds }.toSet()
            val associationEntitySetIds = associationFilterDefinitions.flatMap { it.entitySetIds }.toSet()

            if ( entitySetIds.isEmpty() || associationEntitySetIds.isEmpty() ){
                return@forEachIndexed
            }

            PostgresIterable<DataEdgeKey>(
                    Supplier {
                        val connection = hds.connection
                        val stmt = connection.createStatement()

                        val srcEdgeView = createSrcEdgesView(
                                index, connection, ids, entitySetIds, associationEntitySetIds
                        )

                        val srcEntityFilteringViews = createSrcFilteringViews(
                                index,
                                connection,
                                entityFilterDefinitions,
                                propertyTypes,
                                authorizedPropertyTypes,
                                propertyTypeFqns
                        )
                        val srcEdgeFilteringView = createSrcEdgeFilteringViews(
                                index,
                                connection,
                                associationFilterDefinitions,
                                propertyTypes,
                                authorizedPropertyTypes,
                                propertyTypeFqns
                        )
                        val edgeJoins = srcEdgeFilteringView.keys.map { " (SELECT id as ${EDGE_COMP_2.name} FROM $it) as $it " }
                        val sql = "SELECT * FROM ${srcEdgeView.first} INNER JOIN " + srcEntityFilteringViews.keys.joinToString(
                                " USING (${ID_VALUE.name}) INNER JOIN "
                        ) + " USING(${ID_VALUE.name}) +" +
                                " INNER JOIN " + edgeJoins.joinToString(
                                " USING (${ID_VALUE.name}) INNER JOIN "
                        ) + " USING (${ID_VALUE.name}) "
                        val rs = stmt.executeQuery(sql)
                        StatementHolder(connection, stmt, rs)
                    }, Function {
                val srcEs = it.getObject(SRC_ENTITY_SET_ID_FIELD, UUID::class.java)
                val srcId = it.getObject(ID_VALUE.name, UUID::class.java)
                val dstEs = it.getObject(DST_ENTITY_SET_ID_FIELD, UUID::class.java)
                val dstId = it.getObject(EDGE_COMP_1_FIELD, UUID::class.java)
                val edgeEs = it.getObject(EDGE_ENTITY_SET_ID_FIELD, UUID::class.java)
                val edgeId = it.getObject(EDGE_COMP_2.name, UUID::class.java)
                DataEdgeKey(EntityDataKey(srcEs, srcId), EntityDataKey(dstId, dstEs), EntityDataKey(edgeEs, edgeId))
            }).forEach {
                entitiesWithoutData.getOrPut(it.src.entitySetId) { mutableSetOf() }.add(it.src.entityKeyId)
                entitiesWithoutData.getOrPut(it.dst.entitySetId) { mutableSetOf() }.add(it.dst.entityKeyId)
                entitiesWithoutData.getOrPut(it.edge.entitySetId) { mutableSetOf() }.add(it.edge.entityKeyId)
                associations
                        .getOrPut(it.dst.entityKeyId) { mutableMapOf() }
                        .getOrPut(it.edge.entitySetId) { mutableMapOf() }[it.src.entitySetId] = NeighborIds(
                        it.edge.entityKeyId,
                        it.src.entityKeyId
                )
            }
        }

        query.dstSelections.forEachIndexed { index, selection ->

            val entityFilterDefinitions = getFilterDefinitions(
                    selection.entityTypeIds,
                    selection.entitySetIds,
                    selection.entityFilters
            )

            val associationFilterDefinitions = getFilterDefinitions(
                    selection.associationTypeIds,
                    selection.associationEntitySetIds,
                    selection.associationFilters
            )

            val entitySetIds = entityFilterDefinitions.flatMap { it.entitySetIds }.toSet()
            val associationEntitySetIds = associationFilterDefinitions.flatMap { it.entitySetIds }.toSet()

            if ( entitySetIds.isEmpty() || associationEntitySetIds.isEmpty() ){
                return@forEachIndexed
            }

            PostgresIterable<DataEdgeKey>(
                    Supplier {
                        val connection = hds.connection
                        val stmt = connection.createStatement()

                        val dstEdgeView = createDstEdgesView(
                                index, connection, ids, entitySetIds, associationEntitySetIds
                        )
                        val dstEntityFilteringViews = createDstFilteringViews(
                                index,
                                connection,
                                entityFilterDefinitions,
                                propertyTypes,
                                authorizedPropertyTypes,
                                propertyTypeFqns
                        )
                        val dstEdgeFilteringView = createDstEdgeFilteringViews(
                                index,
                                connection,
                                associationFilterDefinitions,
                                propertyTypes,
                                authorizedPropertyTypes,
                                propertyTypeFqns
                        )
                        val edgeJoins = dstEdgeFilteringView.keys.map { " (SELECT id as ${EDGE_COMP_2.name} FROM $it) as $it " }
                        val sql = "SELECT * FROM ${dstEdgeView.first} INNER JOIN " + dstEntityFilteringViews.keys.joinToString(
                                " USING (${ID_VALUE.name}) INNER JOIN "
                        ) + " USING(${ID_VALUE.name}) +" +
                                " INNER JOIN " + edgeJoins.joinToString(
                                " USING (${ID_VALUE.name}) INNER JOIN "
                        ) + " USING (${ID_VALUE.name}) "
                        val rs = stmt.executeQuery(sql)
                        StatementHolder(connection, stmt, rs)
                    }, Function {
                val srcEs = it.getObject(SRC_ENTITY_SET_ID_FIELD, UUID::class.java)
                val srcId = it.getObject(EDGE_COMP_2_FIELD, UUID::class.java)
                val dstEs = it.getObject(DST_ENTITY_SET_ID_FIELD, UUID::class.java)
                val dstId = it.getObject(ID_VALUE.name, UUID::class.java)
                val edgeEs = it.getObject(EDGE_ENTITY_SET_ID_FIELD, UUID::class.java)
                val edgeId = it.getObject(EDGE_COMP_1_FIELD, UUID::class.java)
                DataEdgeKey(EntityDataKey(srcEs, srcId), EntityDataKey(dstId, dstEs), EntityDataKey(edgeEs, edgeId))
            }).forEach {
                entitiesWithoutData.getOrPut(it.src.entitySetId) { mutableSetOf() }.add(it.src.entityKeyId)
                entitiesWithoutData.getOrPut(it.dst.entitySetId) { mutableSetOf() }.add(it.dst.entityKeyId)
                entitiesWithoutData.getOrPut(it.edge.entitySetId) { mutableSetOf() }.add(it.edge.entityKeyId)
                associations
                        .getOrPut(it.src.entityKeyId) { mutableMapOf() }
                        .getOrPut(it.edge.entitySetId) { mutableMapOf() }[it.dst.entitySetId] = NeighborIds(
                        it.edge.entityKeyId,
                        it.dst.entityKeyId
                )
            }

        }
        //Now just need to return the union of both these selections

//        check( entitySetIds.isNotEmpty() ) {"Entity set ids cannot be empty."}
//        check( associationEntitySetIds.isNotEmpty() ) {"Association entity set ids cannot be empty."}
        return neighborhood
    }

    private fun createDstEdgeFilteringViews(
            index: Int,
            connection: Connection,
            associationFilterDefinitions: List<AssociationFilterDefinition>,
            propertyTypes: Map<UUID, PropertyType>,
            authorizedPropertyTypes: Map<UUID, Set<UUID>>,
            propertyTypeFqns: Map<UUID, String>
    ): Map<String, Statement> {

        return mapOf()
    }

    private fun createDstFilteringViews(
            index: Int,
            connection: Connection,
            entityFilterDefinitions: List<AssociationFilterDefinition>,
            propertyTypes: Map<UUID, PropertyType>,
            authorizedPropertyTypes: Map<UUID, Set<UUID>>,
            propertyTypeFqns: Map<UUID, String>
    ): Map<String, Statement> {

        return mapOf()
    }

    private fun createDstEdgesView(
            index: Int,
            connection: Connection,
            ids: Set<UUID>,
            entitySetIds: Set<UUID>,
            associationEntitySetIds: Set<UUID>
    ): Pair<String, Statement> {
        val tableName = "tmp_src_edges$index"
        val tableSql = buildEdgesOutgoingJoinSql(ids, entitySetIds, associationEntitySetIds)

        val stmt = connection.createStatement()
        stmt.executeQuery(tableSql)

        return tableName to stmt
    }

    private fun createSrcEdgesView(
            index: Int,
            connection: Connection,
            ids: Set<UUID>,
            entitySetIds: Set<UUID>,
            associationEntitySetIds: Set<UUID>
    ): Pair<String, Statement> {
        val tableName = "tmp_src_edges_$index"
        val tableSql = buildEdgesIncomingJoinSql(ids, entitySetIds, associationEntitySetIds)

        val stmt = connection.createStatement()
        stmt.executeQuery(tableSql)

        return tableName to stmt
    }

    private fun createSrcEdgeFilteringViews(
            index: Int,
            connection: Connection,
            filterDefinitions: List<AssociationFilterDefinition>,
            propertyTypes: Map<UUID, PropertyType>,
            authorizedPropertyTypes: Map<UUID, Set<UUID>>,
            propertyTypeFqns: Map<UUID, String>
    ): Map<String, Statement> {
        //We're able to re-use SrcFiltering views with a simple table name change because it's just entity tables.
        return filterDefinitions.mapIndexed { filterIndex, filterDefinition ->
            val tableName = "tmp_src_edge_${index}_$filterIndex"
            val stmt = connection.createStatement()
            stmt.executeQuery(
                    createSrcFilteringViews(
                            tableName,
                            filterDefinition,
                            propertyTypes,
                            authorizedPropertyTypes,
                            propertyTypeFqns
                    )
            )
            return@mapIndexed tableName to stmt
        }.toMap()
    }

    private fun getFilterDefinitions(
            maybeEntityTypeIds: Optional<Set<UUID>>,
            maybeEntitySetIds: Optional<Set<UUID>>,
            maybeFilters: Optional<Map<UUID, Map<UUID, Set<Filter>>>>
    ): List<AssociationFilterDefinition> {
        val entitySetsByType = getEntitySetsByEntityTypeIds(maybeEntityTypeIds)
        val entitySetsWithType = maybeEntitySetIds.map { entitySetIds ->
            edm.getEntityTypeIdsByEntitySetIds(entitySetIds)
        }.orElse(emptyMap())

        return maybeFilters.map { filters ->
            entitySetsByType.map { (entityTypeId, entitySetIds) ->
                AssociationFilterDefinition(entityTypeId, entitySetIds, getFilters(entitySetIds, filters))
            } + entitySetsWithType.map { (entitySetId, entityTypeId) ->
                val entitySetIds = setOf(entitySetId)
                AssociationFilterDefinition(entityTypeId, entitySetIds, getFilters(entitySetIds, filters))
            }
        }.orElse(emptyList())

    }

    private fun getFilters(
            entitySetIds: Set<UUID>,
            filters: Map<UUID, Map<UUID, Set<Filter>>>
    ): MutableMap<UUID, MutableSet<Filter>> {
        val mergedFilters = mutableMapOf<UUID, MutableSet<Filter>>()
        entitySetIds.map { entitySetId ->
            filters[entitySetId]?.forEach { propertyTypeId, filter ->
                mergedFilters.getOrPut(propertyTypeId) { mutableSetOf() }.addAll(filter)
            }
        }
        return mergedFilters
    }

    private fun createSrcFilteringViews(
            index: Int,
            connection: Connection,
            filterDefinitions: List<AssociationFilterDefinition>,
            propertyTypes: Map<UUID, PropertyType>,
            authorizedPropertyTypes: Map<UUID, Set<UUID>>,
            propertyTypeFqns: Map<UUID, String>
    ): Map<String, Statement> {
        return filterDefinitions.mapIndexed { filterIndex, filterDefinition ->
            val tableName = "tmp_src_${index}_$filterIndex"
            val stmt = connection.createStatement()
            stmt.executeQuery(
                    createSrcFilteringViews(
                            tableName,
                            filterDefinition,
                            propertyTypes,
                            authorizedPropertyTypes,
                            propertyTypeFqns
                    )
            )
            return@mapIndexed tableName to stmt
        }.toMap()
    }

    private fun createSrcFilteringViews(
            tableName: String,
            filterDefinition: AssociationFilterDefinition,
            propertyTypes: Map<UUID, PropertyType>,
            authorizedPropertyTypes: Map<UUID, Set<UUID>>,
            propertyTypeFqns: Map<UUID, String>
    ): String {
        val tableSql = selectEntitySetWithCurrentVersionOfPropertyTypes(
                filterDefinition.entitySetIds.associateWith { Optional.empty<Set<UUID>>() },
                propertyTypeFqns,
                setOf(),
                authorizedPropertyTypes,
                filterDefinition.filters,
                EnumSet.noneOf(MetadataOption::class.java),
                propertyTypes.mapValues { it.value.datatype == EdmPrimitiveTypeKind.Binary },
                linking = false,
                omitEntitySetId = true
        )
        return "CREATE TEMPORARY VIEW $tableName AS $tableSql"
    }

    /**
     * Used to create unbound prepared statement for generating a edge table fragment useful for joining to source
     * constraints.
     */
    private fun buildSrcJoinSql(
            ids: Set<UUID>,
            entitySetIds: Set<UUID>,
            associationEntitySetIds: Set<UUID>
    ): String {
        val idsClause = "${EDGE_COMP_1.name} ${inClause(ids)}"
        val srcEntitySetIdsClause = "AND ${SRC_ENTITY_SET_ID.name} ${inClause(entitySetIds)}"
        val associationEntitySetIdsClause = "AND ${EDGE_ENTITY_SET_ID.name} ${inClause(associationEntitySetIds)}"
        val componentTypeClause = "AND ${COMPONENT_TYPES.name} = ${ComponentType.SRC.ordinal}"

        //For this there is no dstEntitySet clause since the target is self.
        return "SELECT * FROM ${EDGES.name} WHERE $idsClause $srcEntitySetIdsClause $associationEntitySetIdsClause $componentTypeClause"
    }

    /**
     * Used to create unbound prepared statement for generating a edge table fragment useful for joining to destination
     * constraints.
     */
    private fun buildDstJoinSql(
            ids: Set<UUID>,
            entitySetIds: Set<UUID>,
            associationEntitySetIds: Set<UUID>
    ): String {
        val idsClause = "${EDGE_COMP_2.name} ${inClause(ids)}"
        val associationEntitySetIdsClause = "AND ${EDGE_ENTITY_SET_ID.name} ${inClause(entitySetIds)}"
        val dstEntitySetIdsClause = "AND ${DST_ENTITY_SET_ID.name} ${inClause(associationEntitySetIds)}"
        val componentTypeClause = "AND ${COMPONENT_TYPES.name} = ${ComponentType.DST.ordinal}"

        return "SELECT * FROM ${EDGES.name} WHERE $idsClause $dstEntitySetIdsClause $associationEntitySetIdsClause $componentTypeClause"
    }

    /**
     * Used to create unbound prepared statement for generating a edge table fragment useful for joining to edge
     * constraints. Since a selection must be uni-directional from center you don't have to worry about
     * intersecting them yet.
     */
    private fun buildEdgesIncomingJoinSql(
            ids: Set<UUID>,
            entitySetIds: Set<UUID>,
            associationEntitySetIds: Set<UUID>
    ): String {
        val idsClause = "${EDGE_COMP_2.name} ${inClause(ids)}"
        val associationEntitySetIdsClause = "AND ${EDGE_ENTITY_SET_ID.name} ${inClause(associationEntitySetIds)}"
        val srcEntitySetIdsClause = "AND ${DST_ENTITY_SET_ID.name} ${inClause(entitySetIds)}"
        val componentTypeClause = "AND ${COMPONENT_TYPES.name} = ${ComponentType.EDGE.ordinal}"

        return "SELECT * FROM ${EDGES.name} WHERE $idsClause $srcEntitySetIdsClause $associationEntitySetIdsClause $componentTypeClause"
    }

    /**
     * Used to create unbound prepared statement for generating a edge table fragment useful for joining to edge
     * constraints. Since a selection must be uni-directional from center you don't have to worry about
     * intersecting them yet.
     */
    private fun buildEdgesOutgoingJoinSql(
            ids: Set<UUID>,
            entitySetIds: Set<UUID>,
            associationEntitySetIds: Set<UUID>
    ): String {
        val idsClause = "${EDGE_COMP_1.name} ${inClause(ids)}"
        val associationEntitySetIdsClause = "AND ${EDGE_ENTITY_SET_ID.name} ${inClause(associationEntitySetIds)}"
        val dstEntitySetIdsClause = "AND ${DST_ENTITY_SET_ID.name} ${inClause(entitySetIds)}"
        val componentTypeClause = "AND ${COMPONENT_TYPES.name} = ${ComponentType.EDGE.ordinal}"

        return "SELECT * FROM ${EDGES.name} WHERE $idsClause $dstEntitySetIdsClause $associationEntitySetIdsClause $componentTypeClause"
    }

    private fun inClause(uuids: Set<UUID>): String {
        check(uuids.isNotEmpty()) { "Ids must be provided." }
        return "IN (" + uuids.joinToString(",") { id -> "'$id'" } + ")"
    }

    private fun getEntitySetsByEntityTypeIds(
            maybeEntityTypeIds: Optional<Set<UUID>>
    ): MutableMap<UUID, MutableSet<UUID>> {
        return maybeEntityTypeIds.map { entityTypeIds ->
            entityTypeIds.associateWith { entityTypeId ->
                edm
                        .getEntitySetsOfType(entityTypeId)
                        .map(EntitySet::getId)
                        .toMutableSet()
            }.toMutableMap()
        }.orElse(mutableMapOf())
    }

    override fun getEntitySets(entityTypeIds: Optional<Set<UUID>>): List<UUID> {
        return entityTypeIds
                .map(edm::getEntitySetsOfType)
                .orElseGet { emptyList() }
                .map(EntitySet::getId)
    }
}

private val idsClause = "${PostgresColumn.ID_VALUE.name} IN ("
private val entitySetIdsClause = "${PostgresColumn.ENTITY_SET_ID.name} IN ("

const val TTL_MILLIS = 10 * 60 * 1000
//private val getQuerySql = "SELECT ${QUERY.name} FROM ${GRAPH_QUERIES.name} WHERE ${QUERY_ID.name} = ?"
private val readGraphQueryState = "SELECT * FROM ${GRAPH_QUERIES.name} WHERE ${QUERY_ID.name} = ?"
//private val insertGraphQuery =
//        "INSERT INTO ${GRAPH_QUERIES.name} (${QUERY_ID.name},${QUERY.name},${STATE.name},${START_TIME.name}) " +
//                "VALUES (?,?,?,?)"