package com.openlattice.hazelcast.serializers

import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest
import com.openlattice.data.EntityDataKey
import com.openlattice.linking.EntityKeyPair
import com.openlattice.linking.EntityLinkingFeedback
import java.util.UUID
import kotlin.random.Random

class EntityLinkingFeedbackStreamSerializerTest
    : AbstractStreamSerializerTest<EntityLinkingFeedbackStreamSerializer, EntityLinkingFeedback>() {

    override fun createSerializer(): EntityLinkingFeedbackStreamSerializer {
        return EntityLinkingFeedbackStreamSerializer()
    }

    override fun createInput(): EntityLinkingFeedback {
        return EntityLinkingFeedback(
                EntityKeyPair(
                        EntityDataKey(UUID.randomUUID(), UUID.randomUUID()),
                        EntityDataKey(UUID.randomUUID(), UUID.randomUUID())),
                Random.nextBoolean())
    }
}