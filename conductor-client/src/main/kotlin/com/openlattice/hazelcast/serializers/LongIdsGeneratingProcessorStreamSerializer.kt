package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.ids.processors.LongIdsGeneratingProcessor
import org.springframework.stereotype.Component


@Component
class LongIdsGeneratingProcessorStreamSerializer : SelfRegisteringStreamSerializer<LongIdsGeneratingProcessor> {
    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.LONG_IDS_GENERATING_PROCESSOR.ordinal
    }

    override fun getClazz(): Class<out LongIdsGeneratingProcessor> {
        return LongIdsGeneratingProcessor::class.java
    }

    override fun write(out: ObjectDataOutput, obj: LongIdsGeneratingProcessor) {
        out.writeLong(obj.count)
    }

    override fun read(input: ObjectDataInput): LongIdsGeneratingProcessor {
        return LongIdsGeneratingProcessor(input.readLong())
    }
}