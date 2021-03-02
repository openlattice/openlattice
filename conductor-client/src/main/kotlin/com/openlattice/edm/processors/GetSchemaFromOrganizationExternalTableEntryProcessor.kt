package com.openlattice.edm.processors

import com.hazelcast.core.Offloadable
import com.openlattice.organization.OrganizationExternalDatabaseTable
import com.openlattice.rhizome.hazelcast.entryprocessors.AbstractReadOnlyRhizomeEntryProcessor
import java.util.*

/**
 * @author Drew Bailey (drew@openlattice.com)
 */
class GetSchemaFromOrganizationExternalTableEntryProcessor:
        AbstractReadOnlyRhizomeEntryProcessor<UUID, OrganizationExternalDatabaseTable, String>(),
        Offloadable
{
    override fun process(entry: MutableMap.MutableEntry<UUID, OrganizationExternalDatabaseTable>): String {
        return entry.value.schema
    }

    override fun getExecutorName(): String {
        return Offloadable.OFFLOADABLE_EXECUTOR
    }
}