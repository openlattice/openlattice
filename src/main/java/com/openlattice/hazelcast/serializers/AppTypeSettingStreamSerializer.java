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

import com.dataloom.mappers.ObjectMappers;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import com.openlattice.apps.AppTypeSetting;
import com.openlattice.authorization.AclKey;
import com.openlattice.authorization.Principal;
import com.openlattice.hazelcast.StreamSerializerTypeIds;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Component
public class AppTypeSettingStreamSerializer implements SelfRegisteringStreamSerializer<AppTypeSetting> {

    private static ObjectMapper mapper = ObjectMappers.getJsonMapper();
    private static TypeReference<Map<String, Object>> typeRef = new TypeReference<Map<String, Object>>() {
    };

    @Override public Class<? extends AppTypeSetting> getClazz() {
        return AppTypeSetting.class;
    }

    @Override public void write( ObjectDataOutput out, AppTypeSetting object ) throws IOException {
        UUIDStreamSerializer.serialize( out, object.getId() );
        UUIDStreamSerializer.serialize( out, object.getEntitySetCollectionId() );

        out.writeInt( object.getRoles().size() );
        for ( Map.Entry<UUID, AclKey> entry : object.getRoles().entrySet() ) {
            UUIDStreamSerializer.serialize( out, entry.getKey() );
            AclKeyStreamSerializer.serialize( out, entry.getValue() );
        }
        out.writeByteArray( mapper.writeValueAsBytes( object.getSettings() ) );
    }

    @Override public AppTypeSetting read( ObjectDataInput in ) throws IOException {
        UUID id = UUIDStreamSerializer.deserialize( in );
        UUID entitySetCollectionId = UUIDStreamSerializer.deserialize( in );

        int roleMapSize = in.readInt();
        Map<UUID, AclKey> roleMap = new HashMap<>( roleMapSize );

        for ( int i = 0; i < roleMapSize; i++ ) {
            UUID roleId = UUIDStreamSerializer.deserialize( in );
            AclKey roleAclKey = AclKeyStreamSerializer.deserialize( in );

            roleMap.put( roleId, roleAclKey );
        }

        Map<String, Object> settings = mapper.readValue( in.readByteArray(), typeRef );

        return new AppTypeSetting( id, entitySetCollectionId, roleMap, settings );
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.APP_TYPE_SETTING.ordinal();
    }

    @Override public void destroy() {

    }
}
