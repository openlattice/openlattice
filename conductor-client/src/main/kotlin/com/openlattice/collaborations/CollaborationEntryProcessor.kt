package com.openlattice.collaborations

import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import org.slf4j.LoggerFactory
import java.util.*

private val logger = LoggerFactory.getLogger(CollaborationEntryProcessor::class.java)

class CollaborationEntryProcessor(
        val update: (Collaboration) -> Result
) : AbstractRhizomeEntryProcessor<UUID, Collaboration, Any?>() {

    override fun process(entry: MutableMap.MutableEntry<UUID, Collaboration?>): Any? {
        val collaboration = entry.value
        if (collaboration != null) {
            val (returnValue, modified) = update(collaboration)
            if (modified) {
                entry.setValue(collaboration)
            }
            return returnValue
        } else {
            logger.warn("Collaboration not found when trying to operate on key {}.", entry.key)
        }
        return null
    }

    data class Result(val value: Any? = null, val modified: Boolean = true)
}





