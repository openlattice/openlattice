package com.openlattice.hazelcast.serializers

import com.auth0.json.mgmt.users.User
import com.dataloom.mappers.ObjectMappers
import com.fasterxml.jackson.module.kotlin.readValue
import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer
import com.openlattice.hazelcast.StreamSerializerTypeIds
import org.springframework.stereotype.Component

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Component
class Auth0UserStreamSerializer : SelfRegisteringStreamSerializer<User> {
    private val mapper = ObjectMappers.newJsonMapper()

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.AUTH0_USER.ordinal
    }

    override fun getClazz(): Class<out User> {
        return User::class.java
    }

    override fun write(out: ObjectDataOutput, user: User) {
        out.writeByteArray(mapper.writeValueAsBytes(user))
    }

    override fun read(input: ObjectDataInput): User {
        return mapper.readValue(input.readByteArray())
    }
}