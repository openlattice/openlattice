package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.linking.EntityKeyPair
import org.springframework.stereotype.Component

@Component
class EntityKeyPairStreamSerializer : SelfRegisteringStreamSerializer<EntityKeyPair> {
    private val entityDataKeyStreamSerializer = EntityDataKeyStreamSerializer()

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.ENTITY_KEY_PAIR.ordinal
    }

    override fun destroy() {}

    override fun getClazz(): Class<out EntityKeyPair> {
        return EntityKeyPair::class.java
    }

    override fun write(out: ObjectDataOutput, value: EntityKeyPair) {
        entityDataKeyStreamSerializer.write(out, value.first)
        entityDataKeyStreamSerializer.write(out, value.second)
    }

    override fun read(input: ObjectDataInput): EntityKeyPair {
        return EntityKeyPair(entityDataKeyStreamSerializer.read(input), entityDataKeyStreamSerializer.read(input))
    }
}