package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.geekbeast.rhizome.hazelcast.serializers.RhizomeUtils
import com.geekbeast.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer
import com.openlattice.edm.processors.AddFlagsOnEntitySetEntryProcessor
import com.openlattice.edm.set.EntitySetFlag
import com.openlattice.hazelcast.StreamSerializerTypeIds

/**
 * @author Drew Bailey (drew@openlattice.com)
 */
class AddFlagsOnEntitySetEntryProcessorStreamSerializer:
    SelfRegisteringStreamSerializer<AddFlagsOnEntitySetEntryProcessor> {
    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.ADD_ENTITY_SET_FLAGS_EP.ordinal
    }

    override fun getClazz(): Class<out AddFlagsOnEntitySetEntryProcessor> {
        return AddFlagsOnEntitySetEntryProcessor::class.java
    }

    override fun write(out: ObjectDataOutput, `object`: AddFlagsOnEntitySetEntryProcessor) {
        RhizomeUtils.Serializers.serializeEnumSet(out, EntitySetFlag::class.java, `object`.flags)
    }

    override fun read(`in`: ObjectDataInput): AddFlagsOnEntitySetEntryProcessor {
        return AddFlagsOnEntitySetEntryProcessor(RhizomeUtils.Serializers.deSerializeEnumSet(`in`, EntitySetFlag::class.java))
    }
}