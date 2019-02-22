package com.openlattice.linking

import com.openlattice.data.EntityDataKey
import com.openlattice.serializer.AbstractJacksonSerializationTest
import java.util.*
import kotlin.random.Random

class EntityLinkingFeedbackSerializationTests : AbstractJacksonSerializationTest<EntityLinkingFeedback>() {
    override fun getSampleData(): EntityLinkingFeedback {
        return EntityLinkingFeedback(
                EntityKeyPair(
                        EntityDataKey(UUID.randomUUID(), UUID.randomUUID()),
                        EntityDataKey(UUID.randomUUID(), UUID.randomUUID())),
                Random.nextBoolean())
    }

    override fun getClazz(): Class<EntityLinkingFeedback> {
        return EntityLinkingFeedback::class.java
    }
}