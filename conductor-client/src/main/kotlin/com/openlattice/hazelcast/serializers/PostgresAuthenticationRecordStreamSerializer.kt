package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializers
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.postgres.PostgresAuthenticationRecord
import com.openlattice.postgres.PostgresConnectionType
import org.springframework.stereotype.Component

@Component
class PostgresAuthenticationRecordStreamSerializer : SelfRegisteringStreamSerializer<PostgresAuthenticationRecord> {

    companion object{
        private val connectionTypes = PostgresConnectionType.values()
        fun serialize(output: ObjectDataOutput, obj: PostgresAuthenticationRecord) {
            output.writeInt(obj.connectionType.ordinal)
            output.writeUTF(obj.database)
            output.writeUTF(obj.username)
            output.writeUTF(obj.ipAddress)
            output.writeUTF(obj.authenticationMethod)
        }

        fun deserialize(input: ObjectDataInput): PostgresAuthenticationRecord {
            val connectionType = connectionTypes[input.readInt()]
            val database = input.readUTF()
            val username = input.readUTF()
            val ipAddress = input.readUTF()
            val authenticationMethod = input.readUTF()
            return PostgresAuthenticationRecord(connectionType, database, username, ipAddress, authenticationMethod)
        }
    }

    override fun write(output: ObjectDataOutput, obj: PostgresAuthenticationRecord) {
        serialize(output, obj)
    }

    override fun read(input: ObjectDataInput): PostgresAuthenticationRecord {
        return deserialize(input)
    }

    override fun getClazz(): Class<out PostgresAuthenticationRecord> {
        return PostgresAuthenticationRecord::class.java
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.POSTGRES_AUTHENTICATION_RECORD.ordinal
    }
}