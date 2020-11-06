package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializers
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.hazelcast.processors.RemoveEntitySetsFromLinkingEntitySetProcessor
import org.springframework.stereotype.Component

@Component
class RemoveEntitySetsFromLinkingEntitySetProcessorStreamSerializer:
        SelfRegisteringStreamSerializer<RemoveEntitySetsFromLinkingEntitySetProcessor> {
    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.REMOVE_ENTITY_SETS_FROM_LINKING_ENTITY_SET_PROCESSOR.ordinal
    }

    override fun destroy() {}

    override fun getClazz(): Class<out RemoveEntitySetsFromLinkingEntitySetProcessor> {
        return RemoveEntitySetsFromLinkingEntitySetProcessor::class.java
    }

    override fun write(out: ObjectDataOutput?, `object`: RemoveEntitySetsFromLinkingEntitySetProcessor?) {
        SetStreamSerializers.fastUUIDSetSerialize( out, `object`!!.entitySetIds )
    }

    override fun read(`in`: ObjectDataInput?): RemoveEntitySetsFromLinkingEntitySetProcessor {
        return RemoveEntitySetsFromLinkingEntitySetProcessor( SetStreamSerializers.fastUUIDSetDeserialize( `in` ) )
    }

}