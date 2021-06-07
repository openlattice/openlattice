package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer
import com.openlattice.assembler.processors.MaterializedEntitySetsRefreshAggregator
import com.openlattice.hazelcast.StreamSerializerTypeIds
import org.springframework.stereotype.Component

@Component
class MaterializedEntitySetsRefreshAggregatorStreamSerializer : SelfRegisteringStreamSerializer<MaterializedEntitySetsRefreshAggregator> {
    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.MATERIALIZED_ENTITY_SETS_REFRESH_AGGREGATOR.ordinal
    }

    override fun getClazz(): Class<out MaterializedEntitySetsRefreshAggregator> {
        return MaterializedEntitySetsRefreshAggregator::class.java
    }

    override fun write(out: ObjectDataOutput, `object`: MaterializedEntitySetsRefreshAggregator) {
        out.writeInt(`object`.refreshableEntitySets.size)
        `object`.refreshableEntitySets.forEach { EntitySetAssemblyKeyStreamSerializer.serialize(out, it) }
    }

    override fun read(`in`: ObjectDataInput): MaterializedEntitySetsRefreshAggregator {
        val size = `in`.readInt()
        return MaterializedEntitySetsRefreshAggregator((0 until size).map { EntitySetAssemblyKeyStreamSerializer.deserialize(`in`) }.toMutableSet())
    }

}