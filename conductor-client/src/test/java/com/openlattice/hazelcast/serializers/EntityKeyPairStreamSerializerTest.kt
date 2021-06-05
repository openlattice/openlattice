package com.openlattice.hazelcast.serializers

import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest
import com.openlattice.data.EntityDataKey
import com.openlattice.linking.EntityKeyPair
import java.util.UUID

class EntityKeyPairStreamSerializerTest : AbstractStreamSerializerTest<EntityKeyPairStreamSerializer, EntityKeyPair>() {
    override fun createSerializer(): EntityKeyPairStreamSerializer {
        return EntityKeyPairStreamSerializer()
    }

    override fun createInput(): EntityKeyPair {
        return EntityKeyPair(
                EntityDataKey(UUID.randomUUID(), UUID.randomUUID()),
                EntityDataKey(UUID.randomUUID(), UUID.randomUUID()))

    }
}