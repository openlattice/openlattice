package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializers
import com.kryptnostic.rhizome.hazelcast.serializers.UUIDStreamSerializerUtils
import com.openlattice.collections.CollectionTemplateType
import com.openlattice.collections.EntityTypeCollection
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.mapstores.TestDataFactory
import org.springframework.stereotype.Component
import java.util.*
import kotlin.collections.LinkedHashSet

@Component
class EntityTypeCollectionStreamSerializer : TestableSelfRegisteringStreamSerializer<EntityTypeCollection> {
    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.ENTITY_TYPE_COLLECTION.ordinal
    }

    override fun getClazz(): Class<out EntityTypeCollection> {
        return EntityTypeCollection::class.java
    }

    override fun write(out: ObjectDataOutput, `object`: EntityTypeCollection) {

        UUIDStreamSerializerUtils.serialize(out, `object`.id)
        FullQualifiedNameStreamSerializer.serialize(out, `object`.type)
        out.writeUTF(`object`.title)
        out.writeUTF(`object`.description)
        SetStreamSerializers.serialize(out, `object`.schemas, FullQualifiedNameStreamSerializer::serialize)

        out.writeInt(`object`.template.size)
        `object`.template.forEach { CollectionTemplateTypeStreamSerializer.serialize(out, it) }

    }

    override fun read(`in`: ObjectDataInput): EntityTypeCollection {
        val id = UUIDStreamSerializerUtils.deserialize(`in`)
        val type = FullQualifiedNameStreamSerializer.deserialize(`in`)
        val title = `in`.readUTF()
        val description = Optional.of(`in`.readUTF())
        val schemas = SetStreamSerializers.deserialize(`in`, FullQualifiedNameStreamSerializer::deserialize)

        val templateSize = `in`.readInt()
        val template = LinkedHashSet<CollectionTemplateType>(templateSize)

        for (i in 0 until templateSize) {
            template.add(CollectionTemplateTypeStreamSerializer.deserialize(`in`))
        }

        return EntityTypeCollection(id, type, title, description, schemas, template)
    }

    override fun generateTestValue(): EntityTypeCollection {
        return TestDataFactory.entityTypeCollection()
    }
}