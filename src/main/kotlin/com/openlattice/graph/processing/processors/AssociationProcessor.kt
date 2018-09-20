package com.openlattice.graph.processing.processors

import org.apache.olingo.commons.api.edm.FullQualifiedName

interface AssociationProcessor {

    fun getNeighbours()

    fun getInputType(): FullQualifiedName
    fun requiredProperties() : Set<FullQualifiedName>
    fun getOutputTypes() : Set<FullQualifiedName>
}