package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializers
import com.kryptnostic.rhizome.hazelcast.serializers.UUIDStreamSerializerUtils
import com.openlattice.organizations.JdbcConnectionParameters
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.mapstores.TestDataFactory
import org.springframework.stereotype.Component
import java.util.Properties;
import java.util.Optional;
import java.io.StringReader;

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
        out.writeUTF(obj._title)
        out.writeUTF(obj.url)
        out.writeUTF(obj.driver)
        out.writeUTF(obj.database)
        out.writeUTF(obj.username)
        out.writeUTF(obj.password)
        out.writeObject(obj.properties)
        out.writeUTF(obj.description)
    }

    override fun read(input: ObjectDataInput): JdbcConnectionParameters {
//        val id = UUIDStreamSerializerUtils.deserialize(input)
//        val title = input.readString()!!
//        val url = input.readString()!!
//        val driver = input.readString()!!
//        val database = input.readString()
//        val username = input.readString()
//        val password = input.readString()
//        val properties = Properties()
//        val PropertpropertyString = input.readObject()
//        if (propertyString != null) properties.load(StringReader(propertyString))
//        val description = Optional.ofNullable(input.readString())


        return input.readObject(JdbcConnectionParameters::class.java)!!

//        return JdbcConnectionParameters(
//            id,
//            title,
//            url,
//            driver,
//            database,
//            username,
//            password,
//            properties,
//            description
//        )
    }
}