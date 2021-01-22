package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.hazelcast.serializers.UUIDStreamSerializerUtils
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer
import com.openlattice.external.JdbcConnections
import com.openlattice.hazelcast.StreamSerializerTypeIds
import org.springframework.stereotype.Component

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Component
class JdbcConnectionsStreamSerializer : SelfRegisteringStreamSerializer<JdbcConnections> {
    override fun getTypeId(): Int = StreamSerializerTypeIds.JDBC_CONNECTIONS.ordinal

    override fun getClazz(): Class<out JdbcConnections> = JdbcConnections::class.java

    companion object {
        @JvmStatic
        fun serialize(out: ObjectDataOutput, obj: JdbcConnections) {
            out.writeInt(obj.size)
            obj.forEach { (dataSourceId, dataSource) ->
                UUIDStreamSerializerUtils.serialize(out, dataSourceId)
                JdbcConnectionStreamSerializer.serialize(out, dataSource)
            }
        }

        @JvmStatic
        fun deserialize(input: ObjectDataInput): JdbcConnections {
            val size = input.readInt()
            return JdbcConnections((0 until size).associate {
                UUIDStreamSerializerUtils.deserialize(input) to JdbcConnectionStreamSerializer.deserialize(input)
            }.toMutableMap())
        }
    }

    override fun write(out: ObjectDataOutput, `object`: JdbcConnections) {
        serialize(out, `object`)
    }

    override fun read(`in`: ObjectDataInput): JdbcConnections {
        return deserialize(`in`)
    }
}