package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.geekbeast.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.linking.EntityKeyPair
import org.springframework.stereotype.Component

@Component
class EntityKeyPairStreamSerializer : SelfRegisteringStreamSerializer<EntityKeyPair> {
    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.ENTITY_KEY_PAIR.ordinal
    }

    override fun destroy() {}

    override fun getClazz(): Class<out EntityKeyPair> {
        return EntityKeyPair::class.java
    }

    override fun write(out: ObjectDataOutput, value: EntityKeyPair) {
        EntityDataKeyStreamSerializer.serialize(out, value.first)
        EntityDataKeyStreamSerializer.serialize(out, value.second)
    }

    override fun read(input: ObjectDataInput): EntityKeyPair {
        return EntityKeyPair(
                EntityDataKeyStreamSerializer.deserialize(input),
                EntityDataKeyStreamSerializer.deserialize(input)
        )
    }
}