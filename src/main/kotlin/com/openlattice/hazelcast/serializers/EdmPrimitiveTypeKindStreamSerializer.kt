package com.openlattice.hazelcast.serializers

import com.openlattice.hazelcast.StreamSerializerTypeIds
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind
import org.springframework.stereotype.Component

@Component
class EdmPrimitiveTypeKindStreamSerializer : AbstractEnumSerializer<EdmPrimitiveTypeKind>() {
    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.EDM_PRIMITIVE_TYPE_KIND.ordinal
    }

    override fun getClazz(): Class<out EdmPrimitiveTypeKind> {
        return EdmPrimitiveTypeKind::class.java
    }
}