/*
 * Copyright (C) 2017. OpenLattice, Inc
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

import com.openlattice.hazelcast.StreamSerializerTypeIds;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import com.openlattice.authorization.AclKey;
import com.openlattice.authorization.Principal;
import com.openlattice.neuron.SignalType;
import com.openlattice.neuron.signals.AuditableSignal;
import java.io.IOException;
import java.util.UUID;
import org.springframework.stereotype.Component;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Component
public class AuditableSignalStreamSerializer implements SelfRegisteringStreamSerializer<AuditableSignal> {
    @Override public Class<AuditableSignal> getClazz() {
        return AuditableSignal.class;
    }

    @Override public void write( ObjectDataOutput out, AuditableSignal object ) throws IOException {
        AclKeyStreamSerializer.serialize( out, object.getAclKey() );
        out.writeUTF( object.getDetails() );
        PrincipalStreamSerializer.serialize( out, object.getPrincipal() );
        SignalTypeStreamSerializer.serialize( out, object.getType() );
        UUIDStreamSerializer.serialize( out, object.getAuditId() );
        UUIDStreamSerializer.serialize( out, object.getBlockId() );
        UUIDStreamSerializer.serialize( out, object.getDataId() );
        UUIDStreamSerializer.serialize( out, object.getTimeId() );
    }

    @Override public AuditableSignal read( ObjectDataInput in ) throws IOException {
        AclKey aclKey = AclKeyStreamSerializer.deserialize( in );
        String details = in.readUTF();
        Principal p = PrincipalStreamSerializer.deserialize( in );
        SignalType signalType = SignalTypeStreamSerializer.deserialize( in );

        UUID auditId = UUIDStreamSerializer.deserialize( in );
        UUID blockId = UUIDStreamSerializer.deserialize( in );
        UUID dataId = UUIDStreamSerializer.deserialize( in );
        UUID timeId = UUIDStreamSerializer.deserialize( in );
        return new AuditableSignal( signalType, aclKey, p, details, auditId, timeId, dataId, blockId );
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.AUDITABLE_SIGNAL.ordinal();
    }

    @Override public void destroy() {

    }
}
