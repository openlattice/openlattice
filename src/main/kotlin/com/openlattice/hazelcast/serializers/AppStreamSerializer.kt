package com.openlattice.hazelcast.serializers

import com.dataloom.mappers.ObjectMappers
import com.fasterxml.jackson.core.type.TypeReference
import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer
import com.openlattice.apps.App
import com.openlattice.hazelcast.StreamSerializerTypeIds
import org.springframework.stereotype.Component
import java.util.*

@Component
class AppStreamSerializer : SelfRegisteringStreamSerializer<App> {

    companion object {
        private val mapper = ObjectMappers.getJsonMapper()
        private val typeRef = object : TypeReference<Map<String, Any>>() {}
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.APP.ordinal
    }

    override fun getClazz(): Class<out App> {
        return App::class.java
    }

    override fun write(out: ObjectDataOutput, `object`: App) {
        UUIDStreamSerializer.serialize(out, `object`.id)
        out.writeUTF(`object`.name)
        out.writeUTF(`object`.title)
        out.writeUTF(`object`.description)
        out.writeUTF(`object`.url)
        UUIDStreamSerializer.serialize(out, `object`.entityTypeCollectionId)

        out.writeInt(`object`.appRoles.size)
        `object`.appRoles.forEach { AppRoleStreamSerializer.serialize(out, it) }
        out.writeByteArray(mapper.writeValueAsBytes(`object`.defaultSettings))
    }

    override fun read(`in`: ObjectDataInput): App {
        val id = Optional.of(UUIDStreamSerializer.deserialize(`in`))
        val name = `in`.readUTF()
        val title = `in`.readUTF()
        val description = Optional.of(`in`.readUTF())
        val url = `in`.readUTF()
        val entityTypeCollectionId = UUIDStreamSerializer.deserialize(`in`)

        val numRoles = `in`.readInt()
        val appRoles = (0 until numRoles).map { AppRoleStreamSerializer.deserialize(`in`) }.toSet()

        val defaultSettings: Optional<Map<String, Any>> = Optional.of(mapper.readValue(`in`.readByteArray(), typeRef))

        return App(id, name, title, description, url, entityTypeCollectionId, appRoles, defaultSettings)

    }
}