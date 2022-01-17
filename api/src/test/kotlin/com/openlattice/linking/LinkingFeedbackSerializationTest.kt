package com.openlattice.linking

import com.openlattice.data.EntityDataKey
import com.geekbeast.serializer.serializer.AbstractJacksonSerializationTest
import java.util.UUID

class LinkingFeedbackSerializationTest : AbstractJacksonSerializationTest<LinkingFeedback>() {
    override fun getSampleData(): LinkingFeedback {
        return LinkingFeedback(
                EntityDataKey(UUID.randomUUID(), UUID.randomUUID()),
                setOf(
                        EntityDataKey(UUID.randomUUID(), UUID.randomUUID()),
                        EntityDataKey(UUID.randomUUID(), UUID.randomUUID())),
                setOf(
                        EntityDataKey(UUID.randomUUID(), UUID.randomUUID()),
                        EntityDataKey(UUID.randomUUID(), UUID.randomUUID()))
        )
    }

    override fun getClazz(): Class<LinkingFeedback> {
        return LinkingFeedback::class.java
    }
}