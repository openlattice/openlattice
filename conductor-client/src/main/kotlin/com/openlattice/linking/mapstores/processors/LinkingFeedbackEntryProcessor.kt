package com.openlattice.linking.mapstores.processors

import com.openlattice.linking.EntityKeyPair
import com.openlattice.linking.EntityLinkingFeedback
import com.geekbeast.rhizome.hazelcast.entryprocessors.AbstractReadOnlyRhizomeEntryProcessor

class LinkingFeedbackEntryProcessor : AbstractReadOnlyRhizomeEntryProcessor<EntityKeyPair, Boolean, EntityLinkingFeedback?>() {
    override fun process(entry: MutableMap.MutableEntry<EntityKeyPair, Boolean?>): EntityLinkingFeedback? {
        if (entry.value == null){
            return null
        }
        return EntityLinkingFeedback(entry.key, entry.value!!)
    }
}