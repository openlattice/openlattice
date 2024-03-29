package com.openlattice.hazelcast.serializers

import com.geekbeast.hazelcast.serializers.TestableSelfRegisteringStreamSerializer
import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.geekbeast.rhizome.hazelcast.serializers.UUIDStreamSerializerUtils
import com.openlattice.organizations.JdbcConnectionParameters
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.mapstores.TestDataFactory
import org.springframework.stereotype.Component
import java.util.Properties;

/**
 * @author Andrew Carter andrew@openlattice.com
 */

@Component
class WarehouseStreamSerializer : TestableSelfRegisteringStreamSerializer<JdbcConnectionParameters> {

    override fun generateTestValue(): JdbcConnectionParameters {
        return TestDataFactory.warehouse()
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.JDBC_CONNECTION_PARAMETERS.ordinal
    }

    override fun getClazz(): Class<out JdbcConnectionParameters> {
        return JdbcConnectionParameters::class.java
    }

    override fun write(out: ObjectDataOutput, obj: JdbcConnectionParameters) {
        UUIDStreamSerializerUtils.serialize(out, obj._id)
        out.writeString(obj._title)
        out.writeString(obj.url)
        out.writeString(obj.driver)
        out.writeString(obj.database)
        out.writeString(obj.username)
        out.writeString(obj.password)
        out.writeObject(obj.properties)
        out.writeString(obj._description)
    }

    override fun read(input: ObjectDataInput): JdbcConnectionParameters {
        val id = UUIDStreamSerializerUtils.deserialize(input)
        val title = input.readString()!!
        val url = input.readString()!!
        val driver = input.readString()!!
        val database = input.readString()!!
        val username = input.readString()!!
        val password = input.readString()!!
        val properties: Properties = input.readObject<Properties>()!!
        val description = input.readString()!!

        return JdbcConnectionParameters(
            id,
            title,
            url,
            driver,
            database,
            username,
            password,
            properties,
            description
        )
    }
}