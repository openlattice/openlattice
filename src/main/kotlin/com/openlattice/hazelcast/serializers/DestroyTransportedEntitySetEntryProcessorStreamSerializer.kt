package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.openlattice.transporter.processors.DestroyTransportedEntitySetEntryProcessor
import org.springframework.stereotype.Component

/**
 * @author Drew Bailey &lt;drew@openlattice.com&gt;
 */
@Component
class DestroyTransportedEntitySetEntryProcessorStreamSerializer: NoOpSelfRegisteringStreamSerializer<DestroyTransportedEntitySetEntryProcessor>() {
    override fun getClazz(): Class<out DestroyTransportedEntitySetEntryProcessor> {
        return DestroyTransportedEntitySetEntryProcessor::class.java
    }

    override fun read(`in`: ObjectDataInput?): DestroyTransportedEntitySetEntryProcessor {
        return DestroyTransportedEntitySetEntryProcessor()
    }
}