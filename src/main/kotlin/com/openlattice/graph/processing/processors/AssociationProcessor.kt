package com.openlattice.graph.processing.processors

import org.apache.olingo.commons.api.edm.FullQualifiedName

interface AssociationProcessor: GraphProcessor {
    /**
     * @return Map of FullQualifiedNames of entity types and property types of source inputs
     */
    fun getSrcInputs(): Map<FullQualifiedName, Set<FullQualifiedName>>  //entity type to properties

    /**
     * @return Map of FullQualifiedNames of property types to alias names to use in query for source inputs
     */
    fun getSrcInputAliases(): Map<FullQualifiedName, String> { return mapOf() } //property fqn to alias name

    /**
     * @return Map of FullQualifiedNames of entity types and property types of edge inputs
     */
    fun getEdgeInputs(): Map<FullQualifiedName, Set<FullQualifiedName>>  //entity type to properties

    /**
     * @return Map of FullQualifiedNames of property types to alias names to use in query for edge inputs
     */
    fun getEdgeInputAliases(): Map<FullQualifiedName, String> { return mapOf() } //property fqn to alias name

    /**
     * @return Map of FullQualifiedNames of entity types and property types of destination inputs
     */
    fun getDstInputs(): Map<FullQualifiedName, Set<FullQualifiedName>>  //entity type to properties

    /**
     * @return Map of FullQualifiedNames of property types to alias names to use in query for destination inputs
     */
    fun getDstInputAliases(): Map<FullQualifiedName, String> { return mapOf() } //property fqn to alias name
}