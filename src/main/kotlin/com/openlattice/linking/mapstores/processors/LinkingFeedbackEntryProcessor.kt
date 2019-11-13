package com.openlattice.linking.mapstores.processors

import com.openlattice.linking.EntityKeyPair
import com.openlattice.linking.EntityLinkingFeedback
import com.openlattice.rhizome.hazelcast.entryprocessors.AbstractReadOnlyRhizomeEntryProcessor

class LinkingFeedbackEntryProcessor : AbstractReadOnlyRhizomeEntryProcessor<EntityKeyPair, Boolean, EntityLinkingFeedback>() {
    override fun process(entry: MutableMap.MutableEntry<EntityKeyPair, Boolean>): EntityLinkingFeedback {
        return EntityLinkingFeedback(entry.key, entry.value)
    }
}