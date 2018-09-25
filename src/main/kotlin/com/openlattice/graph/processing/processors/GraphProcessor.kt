package com.openlattice.graph.processing.processors

import com.openlattice.analysis.requests.ValueFilter
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.springframework.transaction.annotation.Propagation
import java.time.OffsetDateTime
import java.util.*

interface GraphProcessor {
    /**
     * @return Map of FullQualifiedNames of entity types and property types which are needed as input for the processor
     */
    fun getInputs(): Map<FullQualifiedName, Set<FullQualifiedName>>  //entity type to properties

    /**
     * @return Pair of FullQualifiedNames of entity type and property type of result of this processor
     */
    fun getOutputs() : Pair<FullQualifiedName,FullQualifiedName> //entity type - property

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
    fun getFilters() : Map<FullQualifiedName, Map<FullQualifiedName, ValueFilter<Any>>>

}