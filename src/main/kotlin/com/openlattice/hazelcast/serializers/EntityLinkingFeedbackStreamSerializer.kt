package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.linking.EntityLinkingFeedback
import org.springframework.stereotype.Component

@Component
class EntityLinkingFeedbackStreamSerializer : SelfRegisteringStreamSerializer<EntityLinkingFeedback> {
    private val entityKeyPairStreamSerializer = EntityKeyPairStreamSerializer()

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.ENTITY_LINKING_FEEDBACK.ordinal
    }

    override fun destroy() {}

    override fun getClazz(): Class<out EntityLinkingFeedback> {
        return EntityLinkingFeedback::class.java
    }

    override fun write(out: ObjectDataOutput, value: EntityLinkingFeedback) {
        entityKeyPairStreamSerializer.write(out, value.entityPair)
        out.writeBoolean(value.linked)
    }

    override fun read(input: ObjectDataInput): EntityLinkingFeedback {
        return EntityLinkingFeedback(entityKeyPairStreamSerializer.read(input), input.readBoolean())
    }
}