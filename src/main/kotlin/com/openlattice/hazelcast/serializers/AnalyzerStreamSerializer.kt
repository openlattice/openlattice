package com.openlattice.hazelcast.serializers

import com.openlattice.edm.type.Analyzer
import com.openlattice.hazelcast.StreamSerializerTypeIds
import org.springframework.stereotype.Component

@Component
class AnalyzerStreamSerializer : AbstractEnumSerializer<Analyzer>() {
    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.ANALYZER.ordinal
    }

    override fun getClazz(): Class<out Analyzer> {
        return Analyzer::class.java
    }
}