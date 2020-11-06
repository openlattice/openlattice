package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer

abstract class NoOpSelfRegisteringStreamSerializer<T>: SelfRegisteringStreamSerializer<T> {
    override fun getTypeId(): Int {
        throw IllegalStateException("$clazz must override fun getTypeId() in NoOpSelfRegisteringStreamSerializer with a unique id from StreamSerializerTypeIds")
    }

    override fun read(`in`: ObjectDataInput?): T {
        return clazz.getConstructor().newInstance()
    }

    override fun write(out: ObjectDataOutput?, `object`: T) {
        // NO-OP
        return
    }
}