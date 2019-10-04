package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer
import com.openlattice.hazelcast.StreamSerializerTypeIds

abstract class NoOpSelfRegisteringStreamSerializer<T>: SelfRegisteringStreamSerializer<T> {

    override fun getTypeId(): Int {
        throw IllegalStateException("$clazz must override fun getTypeId() in NoOpSelfRegisteringStreamSerializer with a unique id from StreamSerializerTypeIds")
    }

    override fun write(out: ObjectDataOutput?, `object`: T) {
        // NO-OP
        return
    }
}