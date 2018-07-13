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

import com.openlattice.graph.query.AbstractBooleanClauses
import com.openlattice.graph.query.BooleanClauses
import com.openlattice.graph.query.ComparisonClause

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class ClauseBuildingVisitor : BooleanClauseVisitorFunction<String> {
    private var sql: String = ""
    private var counter = 1
    private val insertionMap: MutableMap<Int, BooleanClauses> = mutableMapOf()

    override fun apply(clauses: BooleanClauses): String {
        return when (clauses) {
            is ComparisonClause -> executeQuery(clauses)
            is AbstractBooleanClauses.And -> executeQuery(clauses)
            is AbstractBooleanClauses.Or -> executeQuery(clauses)
            else -> throw IllegalStateException("Unrecognized Boolean Clause: ${this.javaClass.name}")
        }
    }

    private fun executeQuery(clauses: AbstractBooleanClauses.Or): String {
        return clauses.childClauses.asSequence().map { apply(it) }.joinToString(" OR ")
    }

    private fun executeQuery(clauses: AbstractBooleanClauses.And): String {
        return clauses.childClauses.asSequence().map { apply(it) }.joinToString(" AND ")
    }

    private fun executeQuery(clauses: ComparisonClause): String {
        insertionMap[counter++] = clauses
        return "${clauses.fqn.fullQualifiedNameAsString} ${clauses.comparisonOp.comparisionString} ?"
    }


}