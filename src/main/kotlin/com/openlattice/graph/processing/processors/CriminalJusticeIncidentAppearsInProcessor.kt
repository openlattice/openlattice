package com.openlattice.graph.processing.processors

import com.openlattice.analysis.requests.ValueFilter
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.springframework.stereotype.Component

private const val source_entity_type = "general.person"
private const val destination_entity_type = "criminaljustice.incident"
private val appearsin_entity_type = "general.appearsin"
private val appearsin_property_type = "ol.personpoliceminutes"

//TODO: need numofpeople property type

//@Component
class CriminalJusticeIncidentAppearsInProcessor:GraphProcessor {
    override fun getFilters(): Map<FullQualifiedName, Map<FullQualifiedName, ValueFilter<Any>>> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getInputs(): Map<FullQualifiedName, Set<FullQualifiedName>> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getOutputs(): Pair<FullQualifiedName, FullQualifiedName> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getSql(): String {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}