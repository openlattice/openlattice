package com.openlattice.graph.processing

import com.openlattice.datastore.services.EdmManager
import com.openlattice.graph.processing.processors.AssociationProcessor
import com.openlattice.graph.processing.processors.GraphProcessor
import com.openlattice.graph.processing.processors.SelfProcessor
import org.apache.olingo.commons.api.edm.FullQualifiedName

class PropagationGraphProcessor(private val edm: EdmManager) {

    val singleForwardPropagationGraph: MutableMap<Propagation, MutableSet<Propagation>> = mutableMapOf()
    val selfPropagationGraph: MutableMap<Propagation, MutableSet<Propagation>> = mutableMapOf()
    val rootInputPropagations: MutableSet<Propagation> = mutableSetOf()
    private val outputPropagations: MutableSet<Propagation> = mutableSetOf()
    private val graphHandler = BaseGraphHandler<Propagation>()

    fun register(processor: GraphProcessor) {
        val inputs = when(processor) {
            is AssociationProcessor -> {
                val allInputs = mutableMapOf<FullQualifiedName, Set<FullQualifiedName>>()
                allInputs.putAll(processor.getSrcInputs())
                allInputs.putAll(processor.getDstInputs())
                allInputs.putAll(processor.getEdgeInputs())
                allInputs
            } is SelfProcessor -> {
                processor.getInputs()
            } else -> {
                mapOf()
            }
        }

        val outputEntityTypeId = edm.getEntityType(processor.getOutput().first).id
        val outputPropertyTypeId = edm.getPropertyTypeId(processor.getOutput().second)

        inputs.map {
            val inputEntityTypeId = edm.getEntityType(it.key).id

            it.value
                    .map { edm.getPropertyTypeId(it) } // List<PropertyType UUID>
                    .map { Propagation(inputEntityTypeId, it) }
                    .toSet()
        }.forEach {
            val outputProp = Propagation(
                    outputEntityTypeId,
                    outputPropertyTypeId)
            it.forEach { // Input propagations
                inputProp ->
                // Add to root propagations, if its not contained in any propagation graph yet
                if(!(outputPropagations.contains(inputProp))) {
                    rootInputPropagations.add(inputProp)
                }
                outputPropagations.add(outputProp)

                // Remove from root propagations if propagation is an output too
                rootInputPropagations.remove(outputProp)

                if(inputProp.entityTypeId == outputEntityTypeId) {
                    selfPropagationGraph.getOrPut(inputProp) { mutableSetOf() }.add(outputProp)
                } else {
                    singleForwardPropagationGraph.getOrPut(inputProp) { mutableSetOf() }.add(outputProp)
                }
            }
        }
    }

    fun hasCycle():Boolean {
        val remain = singleForwardPropagationGraph.toMutableMap()
        remain.putAll(selfPropagationGraph)
        val roots = rootInputPropagations.toMutableSet()

        return graphHandler.hasCycle(remain, roots)
    }
}