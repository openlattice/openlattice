package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.transporter.processors.MarkEntitySetNotTransportedEntryProcessor
import org.springframework.stereotype.Component

/**
 * @author Drew Bailey &lt;drew@openlattice.com&gt;
 */
@Component
class DestroyTransportedEntitySetEntryProcessorStreamSerializer : NoOpSelfRegisteringStreamSerializer<MarkEntitySetNotTransportedEntryProcessor>() {

    override fun getClazz(): Class<out MarkEntitySetNotTransportedEntryProcessor> {
        return MarkEntitySetNotTransportedEntryProcessor::class.java
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.DESTROY_TRANSPORTED_ENTITY_SET_EP.ordinal
    }

    override fun read(`in`: ObjectDataInput?): MarkEntitySetNotTransportedEntryProcessor {
        return MarkEntitySetNotTransportedEntryProcessor()
    }
}