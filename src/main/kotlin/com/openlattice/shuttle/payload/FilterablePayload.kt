package com.openlattice.shuttle.payload

import java.util.function.Predicate

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class FilterablePayload(path: String, vararg filters: Predicate<Map<String, Any?>>) : CsvPayload(path) {
    private val filters = filters

    override fun getPayload(): Iterable<Map<String, Any?>> {
        return super.getPayload().filter { row ->
            filters.all { it.test(row) }
        }
    }
}


fun equalPredicate(colName: String, value: String): Predicate<Map<String, Any?>> {
    return Predicate { row -> value == row.get(colName) }
}

fun notEqualPredicate(colName: String, value: String): Predicate<Map<String, Any?>> {
    return Predicate { row -> row.get(colName) != value }
}