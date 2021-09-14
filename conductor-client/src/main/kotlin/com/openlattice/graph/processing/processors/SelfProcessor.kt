package com.openlattice.graph.processing.processors

import org.apache.olingo.commons.api.edm.FullQualifiedName

interface SelfProcessor:GraphProcessor {
    /**
     * @return Map of FullQualifiedNames of entity types and property types which are needed as input for the processor
     */
    fun getInputs(): Map<FullQualifiedName, Set<FullQualifiedName>>  //entity type to properties
}