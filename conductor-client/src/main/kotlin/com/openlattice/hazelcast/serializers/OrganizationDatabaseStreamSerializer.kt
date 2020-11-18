package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.mapstores.TestDataFactory
import com.openlattice.organizations.OrganizationDatabase
import org.springframework.stereotype.Component

@Component
class OrganizationDatabaseStreamSerializer : TestableSelfRegisteringStreamSerializer<OrganizationDatabase> {

    override fun generateTestValue(): OrganizationDatabase {
        return TestDataFactory.organizationDatabase()
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.ORGANIZATION_DATABASE.ordinal
    }

    override fun getClazz(): Class<out OrganizationDatabase> {
        return OrganizationDatabase::class.java
    }

    override fun write(out: ObjectDataOutput, `object`: OrganizationDatabase) {
        out.writeInt(`object`.oid)
        out.writeUTF(`object`.name)
    }

    override fun read(`in`: ObjectDataInput): OrganizationDatabase {
        val oid = `in`.readInt()
        val name = `in`.readUTF()
        return OrganizationDatabase(oid, name)
    }
}