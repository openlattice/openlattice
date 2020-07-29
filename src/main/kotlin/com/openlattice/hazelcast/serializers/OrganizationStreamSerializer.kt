package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializers
import com.kryptnostic.rhizome.hazelcast.serializers.UUIDStreamSerializerUtils
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.hazelcast.serializers.StreamSerializers.Companion.deserializeMapMap
import com.openlattice.hazelcast.serializers.StreamSerializers.Companion.serializeMapMap
import com.openlattice.organizations.Grant
import com.openlattice.organizations.GrantType
import com.openlattice.organizations.Organization
import org.springframework.stereotype.Component
import java.util.*

@Component
class OrganizationStreamSerializer: SelfRegisteringStreamSerializer<Organization> {

    companion object {
        @JvmStatic
        fun serialize(out: ObjectDataOutput, obj: Organization) {
            OrganizationPrincipalStreamSerializer.serialize(out, obj.securablePrincipal)
            SetStreamSerializers.fastStringSetSerialize(out, obj.emailDomains)
            StreamSerializers.serializeSet(out, obj.members) { principal ->
                PrincipalStreamSerializer.serialize(out, principal)
            }
            StreamSerializers.serializeSet(out, obj.roles) { role ->
                RoleStreamSerializer.serialize(out, role)
            }
            StreamSerializers.serializeSet(out, obj.smsEntitySetInfo) { o ->
                SmsEntitySetInformationStreamSerializer.serialize(out, o)
            }
            out.writeIntArray(obj.partitions.toIntArray())
            SetStreamSerializers.fastUUIDSetSerialize(out, obj.apps)
            SetStreamSerializers.fastStringSetSerialize(out, obj.connections)

            serializeMapMap(out, obj.grants,
                            { key: UUID -> UUIDStreamSerializerUtils.serialize(out, key) },
                            { subKey: GrantType -> GrantTypeStreamSerializer.serialize(out, subKey) },
                            { `val`: Grant -> GrantStreamSerializer.serialize(out, `val`) })
        }

        @JvmStatic
        fun deserialize(input: ObjectDataInput): Organization {
            val op = OrganizationPrincipalStreamSerializer.deserialize(input)
            val email = SetStreamSerializers.fastStringSetDeserialize(input)
            val members = StreamSerializers.deserializeSet(input) {
                PrincipalStreamSerializer.deserialize(input)
            }
            val roles = StreamSerializers.deserializeSet(input) {
                RoleStreamSerializer.deserialize(input)
            }
            val sms = StreamSerializers.deserializeSet(input) {
                SmsEntitySetInformationStreamSerializer.deserialize(input)
            }
            val partitions = StreamSerializers.deserializeIntList(input, mutableListOf()) as MutableList<Int>
            val apps = SetStreamSerializers.fastUUIDSetDeserialize(input)
            val connections = SetStreamSerializers.fastStringSetDeserialize(input)

            val grants = deserializeMapMap(input,
                    mutableMapOf(),
                    { UUIDStreamSerializerUtils.deserialize(input) },
                    { GrantTypeStreamSerializer.deserialize(input) },
                    { GrantStreamSerializer.deserialize(input) })
            return Organization(op, email, members, roles, sms, partitions, apps, connections, grants)
        }
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.ORGANIZATION.ordinal
    }

    override fun getClazz(): Class<out Organization> {
        return Organization::class.java
    }

    override fun write(out: ObjectDataOutput, obj: Organization) {
        serialize(out, obj)
    }

    override fun read(input: ObjectDataInput): Organization {
        return deserialize(input)
    }
}
