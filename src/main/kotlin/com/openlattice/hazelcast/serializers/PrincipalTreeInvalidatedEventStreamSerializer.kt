package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.principals.PrincipalTreeInvalidatedEvent
import org.apache.commons.lang3.RandomStringUtils
import org.springframework.stereotype.Component

@Component
class PrincipalTreeInvalidatedEventStreamSerializer : TestableSelfRegisteringStreamSerializer<PrincipalTreeInvalidatedEvent> {
    override fun generateTestValue(): PrincipalTreeInvalidatedEvent {
        return PrincipalTreeInvalidatedEvent(RandomStringUtils.random(5))
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.PRINCIPAL_TREE_INVALIDATED_EVENT.ordinal
    }

    override fun getClazz(): Class<out PrincipalTreeInvalidatedEvent> {
        return PrincipalTreeInvalidatedEvent::class.java
    }

    override fun write(out: ObjectDataOutput, obj: PrincipalTreeInvalidatedEvent) {
        out.writeUTF(obj.principalId)
    }

    override fun read(input: ObjectDataInput): PrincipalTreeInvalidatedEvent {
        return PrincipalTreeInvalidatedEvent(input.readUTF())
    }

}