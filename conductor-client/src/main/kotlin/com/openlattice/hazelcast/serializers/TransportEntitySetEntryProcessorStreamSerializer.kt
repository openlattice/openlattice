package com.openlattice.hazelcast.serializers

import com.google.common.collect.Maps
import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.hazelcast.serializers.UUIDStreamSerializerUtils
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.transporter.processors.TransportEntitySetEntryProcessor
import com.openlattice.transporter.types.TransporterDatastore
import com.openlattice.transporter.types.TransporterDependent
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.springframework.stereotype.Component
import java.util.ArrayList
import java.util.UUID

/**
 * @author Drew Bailey &lt;drew@openlattice.com&gt;
 */
@Component
class TransportEntitySetEntryProcessorStreamSerializer:
        TestableSelfRegisteringStreamSerializer<TransportEntitySetEntryProcessor> ,
        TransporterDependent<Void?>
{
    @Transient
    private lateinit var data: TransporterDatastore

    override fun write(out: ObjectDataOutput, `object`: TransportEntitySetEntryProcessor) {
        out.writeInt(`object`.ptIdToFqnColumns.size)
        `object`.ptIdToFqnColumns.forEach { id, fqn ->
            UUIDStreamSerializerUtils.serialize(out, id)
            FullQualifiedNameStreamSerializer.serialize(out, fqn)
        }
        UUIDStreamSerializerUtils.serialize(out, `object`.organizationId)
        out.writeInt(`object`.usersToColumnPermissions.size)
        `object`.usersToColumnPermissions.forEach { (key, vals) ->
            out.writeUTF(key)
            out.writeInt(vals.size)
            vals.forEach {
                out.writeUTF( it )
            }
        }
    }

    override fun read(`in`: ObjectDataInput): TransportEntitySetEntryProcessor {
        val colSize = `in`.readInt()
        val columns = Maps.newLinkedHashMapWithExpectedSize<UUID, FullQualifiedName>( colSize )
        for ( i in 0 until colSize ) {
            columns.put(
                    UUIDStreamSerializerUtils.deserialize(`in`),
                    FullQualifiedNameStreamSerializer.deserialize(`in`)
            )
        }
        val orgId = UUIDStreamSerializerUtils.deserialize(`in`)
        val size = `in`.readInt()
        val usersToColPerms = Maps.newLinkedHashMapWithExpectedSize<String, List<String>>(size)
        var innerSize: Int
        for ( i in 0 until size ) {
            val key = `in`.readUTF()
            innerSize = `in`.readInt()
            val list = ArrayList<String>(innerSize)
            for ( j in 0 until innerSize ) {
                list.add(`in`.readUTF())
            }
            usersToColPerms[key] = list
        }
        return TransportEntitySetEntryProcessor(columns, orgId, usersToColPerms).init(data)
    }

    override fun generateTestValue(): TransportEntitySetEntryProcessor {
        return TransportEntitySetEntryProcessor(
                mapOf(),
                UUID.randomUUID(),
                mapOf()
        )
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.TRANSPORT_ENTITY_SET_EP.ordinal
    }

    override fun getClazz(): Class<out TransportEntitySetEntryProcessor> {
        return TransportEntitySetEntryProcessor::class.java
    }

    override fun init(data: TransporterDatastore): Void? {
        this.data = data
        return null
    }
}