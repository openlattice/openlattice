/*
 * Copyright (C) 2019. OpenLattice, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@openlattice.com
 *
 *
 */

package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.hazelcast.serializers.ListStreamSerializers
import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializers
import com.kryptnostic.rhizome.hazelcast.serializers.UUIDStreamSerializerUtils
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer
import com.openlattice.auditing.AuditRecordEntitySetConfiguration
import com.openlattice.hazelcast.StreamSerializerTypeIds
import org.springframework.stereotype.Component

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Component
class AuditRecordEntitySetsConfigurationStreamSerializer : SelfRegisteringStreamSerializer<AuditRecordEntitySetConfiguration> {
    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.AUDIT_RECORD_ENTITY_SET_CONFIGURATION.ordinal
    }

    override fun destroy() {

    }

    override fun getClazz(): Class<AuditRecordEntitySetConfiguration> {
        return AuditRecordEntitySetConfiguration::class.java
    }

    override fun write(out: ObjectDataOutput, obj: AuditRecordEntitySetConfiguration) {
        UUIDStreamSerializerUtils.serialize(out, obj.activeAuditRecordEntitySetId)
        val hasAuditEdgeEntitySetId = (obj.activeAuditEdgeEntitySetId != null)
        out.writeBoolean(hasAuditEdgeEntitySetId)
        if (hasAuditEdgeEntitySetId) {
            UUIDStreamSerializerUtils.serialize(out, obj.activeAuditEdgeEntitySetId)
        }
        SetStreamSerializers.fastUUIDSetSerialize(out, obj.auditRecordEntitySetIds)
        SetStreamSerializers.fastUUIDSetSerialize(out, obj.auditEdgeEntitySetIds)
    }

    override fun read(input: ObjectDataInput): AuditRecordEntitySetConfiguration {
        val activeAuditRecordEntitySetId = UUIDStreamSerializerUtils.deserialize(input)
        val hasAuditEdgeEntitySetId = input.readBoolean()
        val activeAuditEdgeEntitySetId = if (hasAuditEdgeEntitySetId) UUIDStreamSerializerUtils.deserialize(input) else null
        val auditRecordEntitySetIds = ListStreamSerializers.fastUUIDArrayDeserialize(input).toMutableList()
        val auditEdgeEntitySetIds = ListStreamSerializers.fastUUIDArrayDeserialize(input).toMutableList()

        return AuditRecordEntitySetConfiguration(
                activeAuditRecordEntitySetId,
                activeAuditEdgeEntitySetId,
                auditRecordEntitySetIds,
                auditEdgeEntitySetIds
        )
    }
}
