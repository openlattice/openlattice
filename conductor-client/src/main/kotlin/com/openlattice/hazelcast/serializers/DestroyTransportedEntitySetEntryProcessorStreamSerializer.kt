package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.transporter.processors.DestroyTransportedEntitySetEntryProcessor
import org.springframework.stereotype.Component

/**
 * @author Drew Bailey &lt;drew@openlattice.com&gt;
 */
@Component
class DestroyTransportedEntitySetEntryProcessorStreamSerializer : NoOpSelfRegisteringStreamSerializer<DestroyTransportedEntitySetEntryProcessor>() {

    override fun getClazz(): Class<out DestroyTransportedEntitySetEntryProcessor> {
        return DestroyTransportedEntitySetEntryProcessor::class.java
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.DESTROY_TRANSPORTED_ENTITY_SET_EP.ordinal
    }

    override fun read(`in`: ObjectDataInput?): DestroyTransportedEntitySetEntryProcessor {
        return DestroyTransportedEntitySetEntryProcessor()
    }
}