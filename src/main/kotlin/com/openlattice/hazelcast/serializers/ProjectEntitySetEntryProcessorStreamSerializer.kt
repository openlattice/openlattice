package com.openlattice.hazelcast.serializers

import com.google.common.collect.Maps
import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.hazelcast.serializers.UUIDStreamSerializerUtils
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.projector.ProjectEntitySetEntryProcessor
import com.openlattice.transporter.types.TransporterColumnSet
import org.springframework.stereotype.Component
import java.util.*

/**
 * @author Drew Bailey &lt;drew@openlattice.com&gt;
 */
@Component
class ProjectEntitySetEntryProcessorStreamSerializer: TestableSelfRegisteringStreamSerializer<ProjectEntitySetEntryProcessor> {
    override fun write(out: ObjectDataOutput, `object`: ProjectEntitySetEntryProcessor) {
        TransporterColumnSetStreamSerializer.serializeColumnSet(out, `object`.columns)
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

    override fun read(`in`: ObjectDataInput): ProjectEntitySetEntryProcessor {
        val columns = TransporterColumnSetStreamSerializer.deserializeColumnSet(`in`)
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
        return ProjectEntitySetEntryProcessor(columns, orgId, usersToColPerms)
    }

    override fun generateTestValue(): ProjectEntitySetEntryProcessor {
        return ProjectEntitySetEntryProcessor(
                TransporterColumnSet(mapOf()),
                UUID.randomUUID(),
                mapOf()
        )
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.PROJECT_ENTITY_SET_EP.ordinal
    }

    override fun getClazz(): Class<out ProjectEntitySetEntryProcessor> {
        return ProjectEntitySetEntryProcessor::class.java
    }
}