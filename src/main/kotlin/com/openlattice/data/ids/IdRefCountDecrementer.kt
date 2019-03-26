package com.openlattice.data.ids

import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.data.EntityKey
import org.slf4j.LoggerFactory

private val logger = LoggerFactory.getLogger(IdRefCountDecrementer::class.java)

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class IdRefCountDecrementer : AbstractRhizomeEntryProcessor<EntityKey, Long, Long>() {
    override fun process(entry: MutableMap.MutableEntry<EntityKey, Long?>): Long {
        val v = entry.value
        return if (v == null) {
            logger.error("This shouldn't ever happen. Something is wrong with ref counting logic.")
            0
        } else {
            val newValue = v.dec()
            //Delete the value if we have counted down to zero.
            entry.setValue(if (newValue == 0L) null else newValue)
            newValue
        }
    }
}