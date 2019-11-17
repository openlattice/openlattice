package com.openlattice.organizations.processors

import com.geekbeast.rhizome.hazelcast.DelegatedIntList
import com.openlattice.organizations.Organization
import com.openlattice.rhizome.hazelcast.entryprocessors.AbstractReadOnlyRhizomeEntryProcessor
import org.slf4j.LoggerFactory
import java.util.*

private val logger = LoggerFactory.getLogger(OrganizationEntryProcessor::class.java)

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class OrganizationPartitionReadEntryProcessor : AbstractReadOnlyRhizomeEntryProcessor<UUID, Organization, DelegatedIntList>() {
    override fun process(entry: MutableMap.MutableEntry<UUID, Organization?>): DelegatedIntList {
        return DelegatedIntList(entry.value?.partitions ?: listOf())
    }
}

