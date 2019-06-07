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

import com.openlattice.analysis.requests.AggregationType
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

class AssemblerQueryService {

    companion object {
        private val logger = LoggerFactory.getLogger(AssemblerQueryService::class.java)
    }

    fun simpleAggregation(connection: Connection,
                          srcEntitySetName: String, edgeEntitySetName: String, dstEntitySetName: String,
                          srcGroupColumns: List<String>, edgeGroupColumns: List<String>, dstGroupColumns: List<String>,
                          srcAggregates: Map<String, List<AggregationType>>, edgeAggregates: Map<String, List<AggregationType>>, dstAggregates: Map<String, List<AggregationType>>
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
        val colAliases = (srcGroupColAliases + edgeGroupColAliases + dstGroupColAliases)
                .joinToString(", ") { DataTables.quote(it) }


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

        val aggregateCols = (srcAggregateCols + edgeAggregateCols + dstAggregateCols).joinToString(", ")


        // Calculations

        val calcualtionAliases = mutableListOf<String>()


        // The query

        val simpleSql = simpleAggregationJoinSql(srcEntitySetName, edgeEntitySetName, dstEntitySetName, cols, colAliases, aggregateCols)

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
            } + (calcualtionAliases).map { col ->
                col to rs.getObject(col)
            }).toMap()
        })
    }


    fun simpleAggregationJoinSql(srcEntitySetName: String, edgeEntitySetName: String, dstEntitySetName: String, cols: String, colAliases: String, aggregateCols: String): String {
        return "SELECT $cols, $aggregateCols FROM ${AssemblerConnectionManager.MATERIALIZED_VIEWS_SCHEMA}.${PostgresTable.EDGES.name} " +
                "INNER JOIN ${AssemblerConnectionManager.MATERIALIZED_VIEWS_SCHEMA}.\"$srcEntitySetName\" AS $SRC_TABLE_ALIAS USING( ${PostgresColumn.ID.name} ) " +
                "INNER JOIN ${AssemblerConnectionManager.MATERIALIZED_VIEWS_SCHEMA}.\"$edgeEntitySetName\" AS $EDGE_TABLE_ALIAS ON( $EDGE_TABLE_ALIAS.${PostgresColumn.ID.name} = ${PostgresColumn.EDGE_COMP_2.name} ) " +
                "INNER JOIN ${AssemblerConnectionManager.MATERIALIZED_VIEWS_SCHEMA}.\"$dstEntitySetName\" AS $DST_TABLE_ALIAS ON( $DST_TABLE_ALIAS.${PostgresColumn.ID.name} = ${PostgresColumn.EDGE_COMP_1.name} ) " +
                "WHERE ${PostgresColumn.COMPONENT_TYPES.name} = 0 " +
                "GROUP BY ($colAliases)"
    }

    class DurationCalculator(val endColumn: String, val startColumn: String) {
        fun firstStart(): String {
            return "(SELECT unnest($startColumn) ORDER BY 1 LIMIT 1)"
        }

        fun lastEnd(): String {
            return "(SELECT unnest($endColumn)) ORDER BY 1 DESC LIMIT 1)"
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

