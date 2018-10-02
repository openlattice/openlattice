package com.openlattice.graph.processing.processors

import org.apache.olingo.commons.api.edm.FullQualifiedName

interface AssociationProcessor: GraphProcessor {
    /**
     * @return Map of FullQualifiedNames of entity types and property types of source inputs
     */
    fun getSrcInputs(): Map<FullQualifiedName, Set<FullQualifiedName>>  //entity type to properties

    /**
     * @return Map of FullQualifiedNames of entity types and property types of edge inputs
     */
    fun getEdgeInputs(): Map<FullQualifiedName, Set<FullQualifiedName>>  //entity type to properties

    /**
     * @return Map of FullQualifiedNames of entity types and property types of destination inputs
     */
    fun getDstInputs(): Map<FullQualifiedName, Set<FullQualifiedName>>  //entity type to properties
}