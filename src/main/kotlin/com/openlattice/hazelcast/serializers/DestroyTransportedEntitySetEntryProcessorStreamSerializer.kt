package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.transporter.processors.DestroyTransportedEntitySetEntryProcessor
import com.openlattice.transporter.types.TransporterDatastore
import com.openlattice.transporter.types.TransporterDependent
import org.springframework.stereotype.Component

/**
 * @author Drew Bailey &lt;drew@openlattice.com&gt;
 */
@Component
class DestroyTransportedEntitySetEntryProcessorStreamSerializer:
        NoOpSelfRegisteringStreamSerializer<DestroyTransportedEntitySetEntryProcessor>(),
        TransporterDependent<Void?>
{

    @Transient
    private lateinit var data: TransporterDatastore

    override fun getClazz(): Class<out DestroyTransportedEntitySetEntryProcessor> {
        return DestroyTransportedEntitySetEntryProcessor::class.java
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.DESTROY_TRANSPORTED_ENTITY_SET_EP.ordinal
    }

    override fun read(`in`: ObjectDataInput?): DestroyTransportedEntitySetEntryProcessor {
        return DestroyTransportedEntitySetEntryProcessor().init(data)
    }

    override fun init(data: TransporterDatastore): Void? {
        this.data = data
        return null
    }
}