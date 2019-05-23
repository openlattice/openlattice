package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer
import com.openlattice.collections.aggregators.EntitySetCollectionConfigAggregator
import com.openlattice.hazelcast.StreamSerializerTypeIds
import org.springframework.stereotype.Component

@Component
class EntitySetCollectionConfigAggregatorStreamSerializer : SelfRegisteringStreamSerializer<EntitySetCollectionConfigAggregator> {

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.ENTITY_SET_COLLECTION_CONFIG_AGGREGATOR.ordinal
    }

    override fun getClazz(): Class<out EntitySetCollectionConfigAggregator> {
        return EntitySetCollectionConfigAggregator::class.java
    }

    override fun write(out: ObjectDataOutput, `object`: EntitySetCollectionConfigAggregator) {
        CollectionTemplatesStreamSerializer.serialize(out, `object`.collectionTemplates)
    }

    override fun read(`in`: ObjectDataInput): EntitySetCollectionConfigAggregator {
        return EntitySetCollectionConfigAggregator(CollectionTemplatesStreamSerializer.deserialize(`in`))
    }
}