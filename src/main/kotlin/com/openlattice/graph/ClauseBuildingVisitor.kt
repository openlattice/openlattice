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

import com.openlattice.edm.type.PropertyType
import com.openlattice.graph.query.AbstractBooleanClauses
import com.openlattice.graph.query.BooleanClauses
import com.openlattice.graph.query.ComparisonClause
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind
import java.util.*

/**
 *
 * This class builds a prepared statement SQL fragment that will be used to filter a entity set before loading it.
 *
 * It will generate the correct form of clauses to account for property multiplicity and should result in queries that
 * can be pushed down to numeric tables.
 *
 * TODO: We should differentiate between large and small text fields. The postgres indexer has a limit on the maximum
 * size of the field it can index thus it will do scans when doing range queries on strings (as we don't index those fields)
 */
class ClauseBuildingVisitor(
        authorizedPropertyTypes: Map<UUID, PropertyType>
) : BooleanClauseVisitorFunction<String> {
    private var sql: String = ""
    private val fqnMap = authorizedPropertyTypes.map { it.value.type to it.value }.toMap()
    //The insertion map is a public field that will be used by the prepared statement binder.
    val clauses: MutableList<Pair<UUID, BooleanClauses>> = mutableListOf()

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
        val propertyType = fqnMap[clauses.fqn]!!
        this.clauses.add(propertyType.id to clauses)

        return if (propertyType.isMultiValued) {
            " ? ${clauses.comparisonOp.arrayComparisonString} ANY(${clauses.fqn.fullQualifiedNameAsString}) "
        } else {
            " ${clauses.fqn.fullQualifiedNameAsString} ${clauses.comparisonOp.comparisonString} ? "
        }
    }

}