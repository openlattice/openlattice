package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.hazelcast.serializers.UUIDStreamSerializerUtils
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.organizations.JdbcConnection
import org.springframework.stereotype.Component
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Component
class JdbcConnectionStreamSerializer:SelfRegisteringStreamSerializer<JdbcConnection> {
    override fun getTypeId(): Int = StreamSerializerTypeIds.JDBC_CONNECTION.ordinal

    override fun getClazz(): Class<out JdbcConnection> = JdbcConnection::class.java

    override fun write(out: ObjectDataOutput, obj: JdbcConnection) {
        serialize(out, obj)
    }

    override fun read(input: ObjectDataInput): JdbcConnection {
        return deserialize(input)
    }

    companion object {
        @JvmStatic
        fun serialize( out: ObjectDataOutput, obj: JdbcConnection) {
            UUIDStreamSerializerUtils.serialize(out, obj.id)
            out.writeUTF(obj.title)
            out.writeUTF(obj.description)
            out.writeUTF( obj.url )
            out.writeUTF(obj.database)
            out.writeUTF(obj.driver)
            out.writeUTF(obj.username)
            out.writeUTF(obj.password)
            out.writeBoolean(obj.roleManagementEnabled)
            out.writeInt( obj.properties.size)
            obj.properties.forEach { k, v ->
                out.writeUTF(k as String)
                out.writeUTF(v as String)
            }
        }
        @JvmStatic
        fun deserialize( input:ObjectDataInput): JdbcConnection {
            val id = UUIDStreamSerializerUtils.deserialize(input)
            val title = input.readUTF()
            val description = input.readUTF()
            val url = input.readUTF()
            val database = input.readUTF()
            val driver = input.readUTF()
            val username = input.readUTF()
            val password = input.readUTF()
            val roleManagementEnabled = input.readBoolean()
            val propertiesSize = input.readInt()
            val properties = Properties(propertiesSize)
            for( i in 0 until propertiesSize) {
                properties[input.readUTF()] = input.readUTF()
            }
            return JdbcConnection(
                    id = Optional.of(id),
                    title = title,
                    description = Optional.of(description),
                    url = url,
                    driver = driver,
                    database = database,
                    username = username,
                    password = password,
                    roleManagementEnabled = roleManagementEnabled,
                    properties = properties
            )
        }
    }

}
