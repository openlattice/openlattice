package com.openlattice.hazelcast.serializers

import com.dataloom.mappers.ObjectMappers
import com.fasterxml.jackson.core.type.TypeReference
import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.hazelcast.serializers.UUIDStreamSerializerUtils
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer
import com.openlattice.apps.App
import com.openlattice.hazelcast.StreamSerializerTypeIds
import org.springframework.stereotype.Component
import java.util.*

@Component
class AppStreamSerializer : SelfRegisteringStreamSerializer<App> {

    companion object {
        private val mapper = ObjectMappers.getJsonMapper()
        private val typeRef = object : TypeReference<MutableMap<String, Any>>() {}
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.APP.ordinal
    }

    override fun getClazz(): Class<out App> {
        return App::class.java
    }

    override fun write(out: ObjectDataOutput, `object`: App) {
        UUIDStreamSerializerUtils.serialize(out, `object`.id)
        out.writeUTF(`object`.name)
        out.writeUTF(`object`.title)
        out.writeUTF(`object`.description)
        out.writeUTF(`object`.url)
        UUIDStreamSerializerUtils.serialize(out, `object`.entityTypeCollectionId)

        out.writeInt(`object`.appRoles.size)
        `object`.appRoles.forEach { AppRoleStreamSerializer.serialize(out, it) }
        out.writeByteArray(mapper.writeValueAsBytes(`object`.defaultSettings))
    }

    override fun read(`in`: ObjectDataInput): App {
        val id = Optional.of(UUIDStreamSerializerUtils.deserialize(`in`))
        val name = `in`.readUTF()
        val title = `in`.readUTF()
        val description = Optional.of(`in`.readUTF())
        val url = `in`.readUTF()
        val entityTypeCollectionId = UUIDStreamSerializerUtils.deserialize(`in`)

        val numRoles = `in`.readInt()
        val appRoles = (0 until numRoles).map { AppRoleStreamSerializer.deserialize(`in`) }.toMutableSet()

        val defaultSettings: MutableMap<String, Any> = mapper.readValue(`in`.readByteArray(), typeRef)

        return App(id, name, title, description, url, entityTypeCollectionId, appRoles, defaultSettings)

    }
}