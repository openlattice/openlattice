

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

import com.openlattice.hazelcast.StreamSerializerTypeIds;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import com.openlattice.data.TicketKey;
import java.io.IOException;
import java.util.UUID;
import org.springframework.stereotype.Component;

@Component
public class TicketKeyStreamSerializer implements SelfRegisteringStreamSerializer<TicketKey> {

    @Override
    public void write( ObjectDataOutput out, TicketKey object ) throws IOException {
        out.writeUTF( object.getPrincipalId() );
        UUIDStreamSerializer.serialize( out, object.getTicket() );
    }

    @Override
    public TicketKey read( ObjectDataInput in ) throws IOException {
        String principalId = in.readUTF();
        UUID ticket = UUIDStreamSerializer.deserialize( in );
        return new TicketKey( principalId, ticket );
    }

    @Override
    public int getTypeId() {
       return StreamSerializerTypeIds.TICKET_KEY.ordinal();
    }

    @Override
    public void destroy() {}

    @Override
    public Class<TicketKey> getClazz() {
        return TicketKey.class;
    }

}
