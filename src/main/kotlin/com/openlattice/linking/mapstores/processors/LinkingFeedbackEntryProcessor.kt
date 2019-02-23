package com.openlattice.linking.mapstores.processors

import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.linking.EntityKeyPair
import com.openlattice.linking.EntityLinkingFeedback

class LinkingFeedbackEntryProcessor : AbstractRhizomeEntryProcessor<EntityKeyPair, Boolean, EntityLinkingFeedback>() {
    override fun process(entry: MutableMap.MutableEntry<EntityKeyPair, Boolean>): EntityLinkingFeedback {
        return EntityLinkingFeedback(entry.key, entry.value)
    }
}