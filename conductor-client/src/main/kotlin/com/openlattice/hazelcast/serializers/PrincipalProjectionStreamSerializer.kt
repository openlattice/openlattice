package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.openlattice.authorization.projections.PrincipalProjection
import com.openlattice.hazelcast.StreamSerializerTypeIds
import org.springframework.stereotype.Component

@Component
class PrincipalProjectionStreamSerializer: NoOpSelfRegisteringStreamSerializer<PrincipalProjection>() {
    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.PRINCIPAL_PROJECTION.ordinal
    }

    override fun getClazz(): Class<out PrincipalProjection> {
        return PrincipalProjection::class.java
    }

    override fun read(`in`: ObjectDataInput?): PrincipalProjection {
        return PrincipalProjection()
    }
}