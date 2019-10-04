package com.openlattice.assembler.processors

import com.hazelcast.core.ReadOnly
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.assembler.OrganizationAssembly
import com.openlattice.rhizome.hazelcast.entryprocessors.AbstractReadOnlyRhizomeEntryProcessor
import java.util.*

class IsAssemblyInitializedEntryProcessor : AbstractReadOnlyRhizomeEntryProcessor<UUID, OrganizationAssembly, Boolean>() {
    override fun process(entry: MutableMap.MutableEntry<UUID, OrganizationAssembly?>): Boolean {
        val orgAssembly = entry.value ?: return false
        return orgAssembly.initialized
    }
}