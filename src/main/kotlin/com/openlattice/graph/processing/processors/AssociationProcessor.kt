package com.openlattice.graph.processing.processors

abstract class AssociationProcessor: GraphProcessor {

    override fun isSelf(): Boolean {
        return false
    }
}