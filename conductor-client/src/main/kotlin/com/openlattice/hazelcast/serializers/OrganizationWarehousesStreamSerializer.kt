package com.openlattice.hazelcast.serializers

import com.openlattice.organizations.OrganizationWarehouse
import com.openlattice.mapstores.TestDataFactory
import com.openlattice.hazelcast.StreamSerializerTypeIds
import org.springframework.stereotype.Component
import com.kryptnostic.rhizome.hazelcast.serializers.UUIDStreamSerializerUtils
import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput

/**
 * @author Andrew Carter andrew@openlattice.com
 */

@Component
class OrganizationWarehousesStreamSerializer : TestableSelfRegisteringStreamSerializer<OrganizationWarehouse> {
    override fun generateTestValue(): OrganizationWarehouse {
        return TestDataFactory.organizationWarehouse()
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.ORGANIZATION_WAREHOUSE.ordinal
    }

    override fun getClazz(): Class<out OrganizationWarehouse> {
        return OrganizationWarehouse::class.java
    }

    override fun write(out: ObjectDataOutput, obj: OrganizationWarehouse) {
        UUIDStreamSerializerUtils.serialize(out, obj.organizationWarehouseId)
        UUIDStreamSerializerUtils.serialize(out, obj.organizationId)
        UUIDStreamSerializerUtils.serialize(out, obj.warehouseKey)
        out.writeString(obj.name)
    }

    override fun read(input: ObjectDataInput): OrganizationWarehouse {
        val id = UUIDStreamSerializerUtils.deserialize(input)
        val organizationId = UUIDStreamSerializerUtils.deserialize(input)
        val warehouseKey = UUIDStreamSerializerUtils.deserialize(input)
        val name = input.readString()!!

        return OrganizationWarehouse(
            id,
            organizationId,
            warehouseKey,
            name
        )
    }
}