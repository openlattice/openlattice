package com.openlattice.linking

import com.hazelcast.projection.Projection

class LinkingFeedbackProjection: Projection<Map.Entry<EntityKeyPair, Boolean>, EntityLinkingFeedback> {
    override fun transform(input: Map.Entry<EntityKeyPair, Boolean>): EntityLinkingFeedback {
        return EntityLinkingFeedback( input )
    }

}
