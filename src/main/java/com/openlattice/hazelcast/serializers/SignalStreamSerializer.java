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
import com.openlattice.neuron.SignalType;
import com.openlattice.neuron.signals.Signal;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import com.openlattice.authorization.AclKey;
import com.openlattice.authorization.Principal;
import java.io.IOException;
import org.springframework.stereotype.Component;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Component
public class SignalStreamSerializer implements SelfRegisteringStreamSerializer<Signal> {
    @Override public Class<Signal> getClazz() {
        return Signal.class;
    }

    @Override public void write( ObjectDataOutput out, Signal object ) throws IOException {
        AclKeyStreamSerializer.serialize( out, object.getAclKey() );
        out.writeUTF( object.getDetails() );
        PrincipalStreamSerializer.serialize( out, object.getPrincipal() );
        SignalTypeStreamSerializer.serialize( out, object.getType() );
    }

    @Override public Signal read( ObjectDataInput in ) throws IOException {
        AclKey aclKey = AclKeyStreamSerializer.deserialize( in );
        String details = in.readUTF();
        Principal p = PrincipalStreamSerializer.deserialize( in );
        SignalType signalType = SignalTypeStreamSerializer.deserialize( in );
        return new Signal( signalType, aclKey, p, details );
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.SIGNAL.ordinal();
    }

    @Override public void destroy() {

    }
}
