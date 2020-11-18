package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.openlattice.edm.type.Analyzer
import com.openlattice.hazelcast.StreamSerializerTypeIds
import org.springframework.stereotype.Component

@Component
class AnalyzerStreamSerializer : AbstractEnumSerializer<Analyzer>() {

    companion object {
        @JvmStatic
        fun serialize(out: ObjectDataOutput, `object`: Analyzer) =  AbstractEnumSerializer.serialize(out, `object`)
        @JvmStatic
        fun deserialize(`in`: ObjectDataInput): Analyzer = deserialize(Analyzer::class.java, `in`)
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.ANALYZER.ordinal
    }

    override fun getClazz(): Class<Analyzer> {
        return Analyzer::class.java
    }
}