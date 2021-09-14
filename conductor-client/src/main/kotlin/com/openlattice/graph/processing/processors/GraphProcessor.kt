package com.openlattice.graph.processing.processors

import com.openlattice.analysis.requests.ValueFilter
import org.apache.olingo.commons.api.edm.FullQualifiedName

interface GraphProcessor {
    /**
     * @return Pair of FullQualifiedNames of entity type and property type of result of this processor
     */
    fun getOutput() : Pair<FullQualifiedName,FullQualifiedName> //entity type - property

    /**
     * This is intended to be combined with the data table of active properties and will be used to insert
     * into output location
     *
     * @return Should return a SQL expression for computation that can used as a column expression.
     *
     */
    fun getSql() : String

    /**
     * @return Should return a map of filter expression for each property type
     */
    fun getFilters() : Map<FullQualifiedName, Map<FullQualifiedName, ValueFilter<*>>>

}