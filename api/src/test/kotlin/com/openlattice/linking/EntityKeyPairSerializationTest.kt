package com.openlattice.linking

import com.openlattice.data.EntityDataKey
import com.openlattice.serializer.AbstractJacksonSerializationTest
import java.util.UUID

class EntityKeyPairSerializationTest : AbstractJacksonSerializationTest<EntityKeyPair>() {
    override fun getSampleData(): EntityKeyPair {
        return EntityKeyPair(
                EntityDataKey(UUID.randomUUID(), UUID.randomUUID()),
                EntityDataKey(UUID.randomUUID(), UUID.randomUUID()))
    }

    override fun getClazz(): Class<EntityKeyPair> {
        return EntityKeyPair::class.java
    }
}