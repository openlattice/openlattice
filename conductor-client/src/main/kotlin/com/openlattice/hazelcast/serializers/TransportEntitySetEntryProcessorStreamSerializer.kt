package com.openlattice.hazelcast.serializers

import com.google.common.collect.Maps
import com.google.common.collect.Sets
import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.hazelcast.serializers.UUIDStreamSerializerUtils
import com.openlattice.edm.PropertyTypeIdFqn
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.transporter.processors.MarkEntitySetTransportedEntryProcessor
import com.openlattice.transporter.types.TransporterDatastore
import com.openlattice.transporter.types.TransporterDependent
import org.springframework.stereotype.Component
import java.util.ArrayList
import java.util.UUID

/**
 * @author Drew Bailey &lt;drew@openlattice.com&gt;
 */
@Component
class TransportEntitySetEntryProcessorStreamSerializer: NoOpSelfRegisteringStreamSerializer<MarkEntitySetTransportedEntryProcessor>() {
    override fun read(`in`: ObjectDataInput?): MarkEntitySetTransportedEntryProcessor {
        return MarkEntitySetTransportedEntryProcessor()
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.TRANSPORT_ENTITY_SET_EP.ordinal
    }

    override fun getClazz(): Class<out MarkEntitySetTransportedEntryProcessor> {
        return MarkEntitySetTransportedEntryProcessor::class.java
    }
}