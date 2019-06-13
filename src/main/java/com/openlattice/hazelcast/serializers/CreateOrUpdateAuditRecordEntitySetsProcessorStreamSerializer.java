/*
 * Copyright (C) 2018. OpenLattice, Inc.
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
 */

package com.openlattice.hazelcast.serializers;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractUUIDStreamSerializer;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import com.openlattice.edm.processors.CreateOrUpdateAuditRecordEntitySetsProcessor;
import com.openlattice.edm.processors.UpdateAuditEdgeEntitySetIdProcessor;
import com.openlattice.hazelcast.StreamSerializerTypeIds;
import java.io.IOException;
import org.springframework.stereotype.Component;

@Component
public class CreateOrUpdateAuditRecordEntitySetsProcessorStreamSerializer
        implements SelfRegisteringStreamSerializer<CreateOrUpdateAuditRecordEntitySetsProcessor> {

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.CREATE_OR_UPDATE_AUDIT_RECORD_ENTITY_SETS_PROCESSOR.ordinal();
    }

    @Override
    public void destroy() {
    }

    @Override
    public Class<CreateOrUpdateAuditRecordEntitySetsProcessor> getClazz() {
        return CreateOrUpdateAuditRecordEntitySetsProcessor.class;
    }

    @Override public void write(
            ObjectDataOutput out, CreateOrUpdateAuditRecordEntitySetsProcessor object ) throws IOException {
        AbstractUUIDStreamSerializer.serialize( out, object.getAuditRecordEntitySetId() );
        AbstractUUIDStreamSerializer.serialize( out, object.getAuditEdgeEntitySetId() );

    }

    @Override public CreateOrUpdateAuditRecordEntitySetsProcessor read( ObjectDataInput in ) throws IOException {
        return new CreateOrUpdateAuditRecordEntitySetsProcessor(
                AbstractUUIDStreamSerializer.deserialize( in ),
                AbstractUUIDStreamSerializer.deserialize( in ) );
    }
}