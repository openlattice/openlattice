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
import com.openlattice.apps.App;
import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.UUID;
import org.springframework.stereotype.Component;

@Component
public class AppStreamSerializer implements SelfRegisteringStreamSerializer<App> {
    @Override public Class<? extends App> getClazz() {
        return App.class;
    }

    @Override public void write( ObjectDataOutput out, App object ) throws IOException {
        UUIDStreamSerializer.serialize( out, object.getId() );
        out.writeUTF( object.getName() );
        out.writeUTF( object.getTitle() );
        out.writeUTF( object.getUrl() );
        out.writeUTF( object.getDescription() );
        out.writeInt( object.getAppTypeIds().size() );
        for ( UUID id : object.getAppTypeIds() ) {
            UUIDStreamSerializer.serialize( out, id );
        }
    }

    @Override public App read( ObjectDataInput in ) throws IOException {
        UUID id = UUIDStreamSerializer.deserialize( in );
        String name = in.readUTF();
        String title = in.readUTF();
        String url = in.readUTF();
        Optional<String> description = Optional.of( in.readUTF() );

        int numConfigTypeIds = in.readInt();
        LinkedHashSet<UUID> configTypeIds = new LinkedHashSet<>();
        for ( int i = 0; i < numConfigTypeIds; i++ ) {
            configTypeIds.add( UUIDStreamSerializer.deserialize( in ) );
        }
        return new App( id, name, title, description, configTypeIds, url );
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.APP.ordinal();
    }

    @Override public void destroy() {

    }
}
