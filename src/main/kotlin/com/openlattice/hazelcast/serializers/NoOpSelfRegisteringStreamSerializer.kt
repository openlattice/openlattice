package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer
import com.openlattice.hazelcast.StreamSerializerTypeIds

abstract class NoOpSelfRegisteringStreamSerializer<T>: SelfRegisteringStreamSerializer<T> {
    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.NO_SERIALIZATION_ENTRY_PROCESSOR.ordinal
    }

    override fun write(out: ObjectDataOutput?, `object`: T) {
        // NO-OP
        return
    }
}