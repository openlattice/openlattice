

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
import com.geekbeast.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import com.openlattice.requests.RequestStatus;
import java.io.IOException;
import org.springframework.stereotype.Component;

@Component
public class RequestStatusStreamSerializer implements SelfRegisteringStreamSerializer<RequestStatus> {

    private static RequestStatus[] values = RequestStatus.values();

    @Override
    public void write( ObjectDataOutput out, RequestStatus object ) throws IOException {

        serialize( out, object );
    }

    @Override
    public RequestStatus read( ObjectDataInput in ) throws IOException {
        return deserialize( in );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.REQUEST_STATUS.ordinal();
    }

    @Override
    public void destroy() {

    }

    @Override
    public Class<RequestStatus> getClazz() {
        return RequestStatus.class;
    }

    public static void serialize( ObjectDataOutput out, RequestStatus object ) throws IOException {
        out.writeInt( object.ordinal() );
    }

    public static RequestStatus deserialize( ObjectDataInput in ) throws IOException {
        return values[ in.readInt() ];
    }

}
