package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.openlattice.hazelcast.StreamSerializerTypeIds
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind
import org.springframework.stereotype.Component

@Component
class EdmPrimitiveTypeKindStreamSerializer : AbstractEnumSerializer<EdmPrimitiveTypeKind>() {

    companion object {
        @JvmStatic
        fun serialize(out: ObjectDataOutput, `object`: EdmPrimitiveTypeKind ) =  AbstractEnumSerializer.serialize(out, `object`)
        @JvmStatic
        fun deserialize(`in`: ObjectDataInput): EdmPrimitiveTypeKind = deserialize(EdmPrimitiveTypeKind::class.java, `in`)
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.EDM_PRIMITIVE_TYPE_KIND.ordinal
    }

    override fun getClazz(): Class<EdmPrimitiveTypeKind> {
        return EdmPrimitiveTypeKind::class.java
    }
}