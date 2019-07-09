package com.openlattice.linking

import com.openlattice.data.EntityDataKey
import com.openlattice.mapstores.TestDataFactory
import com.openlattice.serializer.AbstractJacksonSerializationTest
import java.util.*
import kotlin.random.Random

class EntityLinkingFeaturesSerializationTest : AbstractJacksonSerializationTest<EntityLinkingFeatures>() {
    override fun getSampleData(): EntityLinkingFeatures {
        return EntityLinkingFeatures(
                EntityLinkingFeedback(
                        EntityKeyPair(
                                EntityDataKey(UUID.randomUUID(), UUID.randomUUID()),
                                EntityDataKey(UUID.randomUUID(), UUID.randomUUID())),
                        Random.nextBoolean()),
                mapOf(
                        TestDataFactory.randomAlphabetic(5) to Random.nextDouble(),
                        TestDataFactory.randomAlphabetic(5) to Random.nextDouble())
        )
    }

    override fun getClazz(): Class<EntityLinkingFeatures> {
        return EntityLinkingFeatures::class.java
    }
}