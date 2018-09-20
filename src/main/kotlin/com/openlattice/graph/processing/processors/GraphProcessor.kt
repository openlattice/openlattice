package com.openlattice.graph.processing.processors

import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.springframework.transaction.annotation.Propagation
import java.time.OffsetDateTime
import java.util.*

interface GraphProcessor {
    /**
     * TODO: Remove
     */
    fun process( entities: Map<UUID, Map<UUID, Map<UUID, Set<Any>>>>, propagationStarted: OffsetDateTime)
    // entity set id -> entity key id ->

    fun getInputs(): Map<FullQualifiedName, Set<FullQualifiedName>>  //entity type to properties
    /**
     *
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
}