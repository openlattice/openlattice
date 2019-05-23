package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer
import com.openlattice.edm.collection.CollectionTemplateKey
import com.openlattice.hazelcast.StreamSerializerTypeIds
import org.springframework.stereotype.Component

@Component
class CollectionTemplateKeyStreamSerializer : SelfRegisteringStreamSerializer<CollectionTemplateKey> {
    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.COLLECTION_TEMPLATE_KEY.ordinal
    }

    override fun getClazz(): Class<out CollectionTemplateKey> {
        return CollectionTemplateKey::class.java
    }

    override fun write(out: ObjectDataOutput, `object`: CollectionTemplateKey) {
        UUIDStreamSerializer.serialize(out, `object`.entitySetCollectionId)
        UUIDStreamSerializer.serialize(out, `object`.templateTypeId)
    }

    override fun read(`in`: ObjectDataInput?): CollectionTemplateKey {
        val entitySetCollectionId = UUIDStreamSerializer.deserialize(`in`)
        val templateTypeId = UUIDStreamSerializer.deserialize(`in`)

        return CollectionTemplateKey(entitySetCollectionId, templateTypeId)
    }


}
