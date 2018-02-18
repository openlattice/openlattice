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
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import com.openlattice.blocking.LinkingEntity;
import com.openlattice.rhizome.hazelcast.DelegatedStringSet;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.springframework.stereotype.Component;

@Component
public class LinkingEntityStreamSerializer implements SelfRegisteringStreamSerializer<LinkingEntity> {

    @Override public void write( ObjectDataOutput out, LinkingEntity object ) throws IOException {
        serialize( out, object );
    }

    @Override public LinkingEntity read( ObjectDataInput in ) throws IOException {
        return deserialize( in );
    }

    public static void serialize( ObjectDataOutput out, LinkingEntity object ) throws IOException {
        Map<UUID, DelegatedStringSet> entity = object.getEntity();
        out.writeInt( entity.size() );
        for ( Map.Entry<UUID, DelegatedStringSet> entry : entity.entrySet() ) {
            UUIDStreamSerializer.serialize( out, entry.getKey() );
            int setSize = entry.getValue().size();
            out.writeInt( setSize );
            for ( String str : entry.getValue() ) {
                out.writeUTF( str );
            }
        }
    }

    public static LinkingEntity deserialize( ObjectDataInput in ) throws IOException {
        Map<UUID, DelegatedStringSet> result = Maps.newHashMap();
        int mapSize = in.readInt();
        for ( int i = 0; i < mapSize; i++ ) {
            UUID id = UUIDStreamSerializer.deserialize( in );
            Set<String> strings = Sets.newHashSet();
            int setSize = in.readInt();
            for ( int j = 0; j < setSize; j++ ) {
                strings.add( in.readUTF() );
            }
            result.put( id, DelegatedStringSet.wrap( strings ) );
        }
        return new LinkingEntity( result );
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.LINKING_ENTITY.ordinal();
    }

    @Override public void destroy() {
    }

    @Override public Class<? extends LinkingEntity> getClazz() {
        return LinkingEntity.class;
    }

}
