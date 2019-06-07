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
package com.openlattice.assembler

import com.openlattice.analysis.requests.*
import com.openlattice.datastore.services.EdmManager
import com.openlattice.postgres.DataTables
import com.openlattice.postgres.PostgresColumn
import com.openlattice.postgres.PostgresTable
import com.openlattice.postgres.streams.PostgresIterable
import com.openlattice.postgres.streams.StatementHolder
import org.slf4j.LoggerFactory
import java.sql.Connection
import java.util.function.Supplier

const val SRC_TABLE_ALIAS = "SRC_TABLE"
const val EDGE_TABLE_ALIAS = "EDGE_TABLE"
const val DST_TABLE_ALIAS = "DST_TABLE"

class AssemblerQueryService(private val edmService: EdmManager) {

    companion object {
        private val logger = LoggerFactory.getLogger(AssemblerQueryService::class.java)
    }

    fun simpleAggregation(connection: Connection,
                          srcEntitySetName: String, edgeEntitySetName: String, dstEntitySetName: String,
                          srcGroupColumns: List<String>, edgeGroupColumns: List<String>, dstGroupColumns: List<String>,
                          srcAggregates: Map<String, List<AggregationType>>, edgeAggregates: Map<String, List<AggregationType>>, dstAggregates: Map<String, List<AggregationType>>,
                          calculations: Set<Calculation>,
                          srcFilters: Map<String, List<Filter>>, edgeFilters: Map<String, List<Filter>>, dstFilters: Map<String, List<Filter>>
    ): Iterable<Map<String, Any>> {

        // Groupings

        val srcGroupColAliases = mutableListOf<String>()
        val edgeGroupColAliases = mutableListOf<String>()
        val dstGroupColAliases = mutableListOf<String>()

        val srcGroupCols = srcGroupColumns.map {
            val alias = "${SRC_TABLE_ALIAS}_$it"
            srcGroupColAliases.add(alias)
            "$SRC_TABLE_ALIAS.${DataTables.quote(it)} AS ${DataTables.quote(alias)}"
        }
        val edgeGroupCols = edgeGroupColumns.map {
            val alias = "${EDGE_TABLE_ALIAS}_$it"
            edgeGroupColAliases.add(alias)
            "$EDGE_TABLE_ALIAS.${DataTables.quote(it)} AS ${DataTables.quote(alias)}"
        }
        val dstGroupCols = dstGroupColumns.map {
            val alias = "${DST_TABLE_ALIAS}_$it"
            edgeGroupColAliases.add(alias)
            "$DST_TABLE_ALIAS.${DataTables.quote(it)} AS ${DataTables.quote(alias)}"
        }

        val cols = (srcGroupCols + edgeGroupCols + dstGroupCols).joinToString(", ")


        // Aggregations

        val srcAggregateAliases = mutableListOf<String>()
        val srcAggregateCols = srcAggregates.flatMap { aggregate ->
            val quotedColumn = DataTables.quote(aggregate.key)
            val aggregateColumn = "$SRC_TABLE_ALIAS.$quotedColumn"
            aggregate.value.map { aggregateFun ->
                val aggregateAlias = "${SRC_TABLE_ALIAS}_${aggregate.key}_$aggregateFun"
                srcAggregateAliases.add(aggregateAlias)
                "$aggregateFun( $aggregateColumn ) AS ${DataTables.quote(aggregateAlias)}"
            }
        }
        val edgeAggregateAliases = mutableListOf<String>()
        val edgeAggregateCols = edgeAggregates.flatMap { aggregate ->
            val quotedColumn = DataTables.quote(aggregate.key)
            val aggregateColumn = "$EDGE_TABLE_ALIAS.$quotedColumn"
            aggregate.value.map { aggregateFun ->
                val aggregateAlias = "${EDGE_TABLE_ALIAS}_${aggregate.key}_$aggregateFun"
                edgeAggregateAliases.add(aggregateAlias)
                "$aggregateFun( $aggregateColumn ) AS ${DataTables.quote(aggregateAlias)}"
            }
        }
        val dstAggregateAliases = mutableListOf<String>()
        val dstAggregateCols = dstAggregates.flatMap { aggregate ->
            val quotedColumn = DataTables.quote(aggregate.key)
            val aggregateColumn = "$DST_TABLE_ALIAS.$quotedColumn"
            aggregate.value.map { aggregateFun ->
                val aggregateAlias = "${DST_TABLE_ALIAS}_${aggregate.key}_$aggregateFun"
                dstAggregateAliases.add(aggregateAlias)
                "$aggregateFun( $aggregateColumn ) AS ${DataTables.quote(aggregateAlias)}"
            }
        }


        // Calculations

        val calculationAliases = mutableListOf<String>()
        val calculationGroupCols = mutableListOf<String>()
        val calculationSqls = calculations
                .filter { it.calculationType.basseType == BaseCalculationTypes.DURATION }
                .map {
                    val firstPropertyType = edmService.getPropertyTypeFqn(it.firstPropertyId.propertyTypeId).fullQualifiedNameAsString
                    val firstPropertyCol = mapOrientationToTableAlias(it.firstPropertyId.orientation) + "." + DataTables.quote(firstPropertyType)
                    val secondPropertyType = edmService.getPropertyTypeFqn(it.secondPropertyId.propertyTypeId).fullQualifiedNameAsString
                    val secondPropertyCol = mapOrientationToTableAlias(it.secondPropertyId.orientation) + "." + DataTables.quote(secondPropertyType)
                    calculationGroupCols.add(firstPropertyCol)
                    calculationGroupCols.add(secondPropertyCol)
                    val calculator = DurationCalculator(firstPropertyCol, secondPropertyCol)
                    val alias = "${it.calculationType}_${firstPropertyType}_$secondPropertyType"
                    calculationAliases.add(alias)
                    mapDurationCalcualtionsToSql(calculator, it.calculationType) + " AS ${DataTables.quote(alias)}"
                }
        // The query

        val groupingColAliases = (srcGroupColAliases + edgeGroupColAliases + dstGroupColAliases)
                .joinToString(", ") { DataTables.quote(it) } + if (calculationGroupCols.isNotEmpty()) {
            calculationGroupCols.joinToString(", ", ", ", "")
        } else {
            ""
        }


        // Filters
        val srcFilterSqls = srcFilters.map {
            val colName = " $SRC_TABLE_ALIAS.${DataTables.quote(it.key)} "
            it.value.map {
                it.asSql(colName)
            }.joinToString(" AND ")
        }

        val edgeFilterSqls = edgeFilters.map {
            val colName = " $EDGE_TABLE_ALIAS.${DataTables.quote(it.key)} "
            it.value.map {
                it.asSql(colName)
            }.joinToString(" AND ")
        }

        val dstFilterSqls = dstFilters.map {
            val colName = " $DST_TABLE_ALIAS.${DataTables.quote(it.key)} "
            it.value.map {
                it.asSql(colName)
            }.joinToString(" AND ")
        }

        val allFilters = (srcFilterSqls + edgeFilterSqls + dstFilterSqls)
        val filtersSql = if (allFilters.isNotEmpty()) allFilters.joinToString(" AND ", " AND ", " ") else ""

        val aggregateCols = (srcAggregateCols + edgeAggregateCols + dstAggregateCols).joinToString(", ")
        val calculationCols = if (calculationSqls.isNotEmpty()) ", " + calculationSqls.joinToString(", ") else ""
        val simpleSql = simpleAggregationJoinSql(srcEntitySetName, edgeEntitySetName, dstEntitySetName,
                cols, groupingColAliases, aggregateCols, calculationCols,
                filtersSql)

        logger.info("Simple assembly aggregate query:\n$simpleSql")

        return PostgresIterable(
                Supplier {
                    val stmt = connection.prepareStatement(simpleSql)
                    val rs = stmt.executeQuery()
                    StatementHolder(connection, stmt, rs)
                }, java.util.function.Function { rs ->
            return@Function ((srcGroupColAliases + edgeGroupColAliases + dstGroupColAliases).map { col ->
                val arrayVal = rs.getArray(col)
                col to if (arrayVal == null) {
                    null
                } else {
                    (rs.getArray(col).array as Array<Any>)[0]
                }
            } + (srcAggregateAliases + edgeAggregateAliases + dstAggregateAliases).map { col ->
                col to rs.getObject(col)
            } + (calculationAliases).map { col ->
                col to rs.getObject(col)
            }).toMap()
        })
    }


    fun simpleAggregationJoinSql(srcEntitySetName: String, edgeEntitySetName: String, dstEntitySetName: String,
                                 cols: String, groupingColAliases: String, aggregateCols: String, calculationCols: String,
                                 filtersSql: String): String {
        return "SELECT $cols, $aggregateCols $calculationCols FROM ${AssemblerConnectionManager.MATERIALIZED_VIEWS_SCHEMA}.${PostgresTable.EDGES.name} " +
                "INNER JOIN ${AssemblerConnectionManager.MATERIALIZED_VIEWS_SCHEMA}.\"$srcEntitySetName\" AS $SRC_TABLE_ALIAS USING( ${PostgresColumn.ID.name} ) " +
                "INNER JOIN ${AssemblerConnectionManager.MATERIALIZED_VIEWS_SCHEMA}.\"$edgeEntitySetName\" AS $EDGE_TABLE_ALIAS ON( $EDGE_TABLE_ALIAS.${PostgresColumn.ID.name} = ${PostgresColumn.EDGE_COMP_2.name} ) " +
                "INNER JOIN ${AssemblerConnectionManager.MATERIALIZED_VIEWS_SCHEMA}.\"$dstEntitySetName\" AS $DST_TABLE_ALIAS ON( $DST_TABLE_ALIAS.${PostgresColumn.ID.name} = ${PostgresColumn.EDGE_COMP_1.name} ) " +
                "WHERE ${PostgresColumn.COMPONENT_TYPES.name} = 0 $filtersSql " +
                "GROUP BY ($groupingColAliases)"
    }

    private fun mapOrientationToTableAlias(orientation: Orientation): String {
        return when (orientation) {
            Orientation.SRC -> SRC_TABLE_ALIAS
            Orientation.EDGE -> EDGE_TABLE_ALIAS
            Orientation.DST -> DST_TABLE_ALIAS
        }
    }

    private fun mapDurationCalcualtionsToSql(durationCalculation: DurationCalculator, calculationType: CalculationType): String {
        return when (calculationType) {
            CalculationType.DURATION_YEAR -> durationCalculation.numberOfYears()
            CalculationType.DURATION_DAY -> durationCalculation.numberOfDays()
            CalculationType.DURATION_HOUR -> durationCalculation.numberOfHours()
        }
    }

    class DurationCalculator(private val endColumn: String, private val startColumn: String) {
        fun firstStart(): String {
            return "(SELECT unnest($startColumn) ORDER BY 1 LIMIT 1)"
        }

        fun lastEnd(): String {
            return "(SELECT unnest($endColumn) ORDER BY 1 DESC LIMIT 1)"
        }

        fun numberOfYears(): String {
            return "SUM(EXTRACT(epoch FROM (${lastEnd()} - ${firstStart()}))/3600/24/365)"
        }

        fun numberOfDays(): String {
            return "SUM(EXTRACT(epoch FROM (${lastEnd()} - ${firstStart()}))/3600/24)"
        }

        fun numberOfHours(): String {
            return "SUM(EXTRACT(epoch FROM (${lastEnd()} - ${firstStart()}))/3600)"
        }

    }
}

