package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.hazelcast.serializers.UUIDStreamSerializerUtils
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer
import com.openlattice.collections.CollectionTemplateKey
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
        UUIDStreamSerializerUtils.serialize(out, `object`.entitySetCollectionId)
        UUIDStreamSerializerUtils.serialize(out, `object`.templateTypeId)
    }

    override fun read(`in`: ObjectDataInput?): CollectionTemplateKey {
        val entitySetCollectionId = UUIDStreamSerializerUtils.deserialize(`in`)
        val templateTypeId = UUIDStreamSerializerUtils.deserialize(`in`)

        return CollectionTemplateKey(entitySetCollectionId, templateTypeId)
    }


}
