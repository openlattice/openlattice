package com.openlattice.edm.processors

import com.openlattice.edm.type.PropertyType
import com.openlattice.rhizome.hazelcast.entryprocessors.AbstractReadOnlyRhizomeEntryProcessor
import org.apache.olingo.commons.api.edm.FullQualifiedName
import java.util.*

/**
 * @author Drew Bailey &lt;drew@openlattice.com&gt;
 */
class GetFqnFromPropertyTypeEntryProcessor: AbstractReadOnlyRhizomeEntryProcessor<UUID, PropertyType, FullQualifiedName>() {
    override fun process(entry: MutableMap.MutableEntry<UUID, PropertyType?>): FullQualifiedName? {
        val es = entry.value ?: return null
        return es.type
    }
}