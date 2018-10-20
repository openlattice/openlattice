package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializers
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.hazelcast.processors.AddEntitySetsToLinkingEntitySetProcessor
import org.springframework.stereotype.Component

@Component
class AddEntitySetsToLinkingEntitySetProcessorStreamSerializer:
        SelfRegisteringStreamSerializer<AddEntitySetsToLinkingEntitySetProcessor> {

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.ADD_ENTITY_SETS_TO_LINKING_ENTITY_SET_PROCESSOR.ordinal
    }

    override fun destroy() { }

    override fun getClazz(): Class<out AddEntitySetsToLinkingEntitySetProcessor> {
        return AddEntitySetsToLinkingEntitySetProcessor::class.java
    }

    override fun write(out: ObjectDataOutput?, `object`: AddEntitySetsToLinkingEntitySetProcessor?) {
        SetStreamSerializers.fastUUIDSetSerialize( out, `object`!!.entitySetIds )
    }

    override fun read(`in`: ObjectDataInput?): AddEntitySetsToLinkingEntitySetProcessor {
        return AddEntitySetsToLinkingEntitySetProcessor( SetStreamSerializers.fastUUIDSetDeserialize( `in` ) )
    }

}