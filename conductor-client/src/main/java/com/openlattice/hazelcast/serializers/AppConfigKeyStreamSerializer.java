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
import com.kryptnostic.rhizome.hazelcast.serializers.UUIDStreamSerializerUtils;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import com.openlattice.apps.AppConfigKey;
import com.openlattice.hazelcast.StreamSerializerTypeIds;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.UUID;

@Component
public class AppConfigKeyStreamSerializer implements SelfRegisteringStreamSerializer<AppConfigKey> {
    @Override public Class<? extends AppConfigKey> getClazz() {
        return AppConfigKey.class;
    }

    @Override public void write( ObjectDataOutput out, AppConfigKey object ) throws IOException {
        UUIDStreamSerializerUtils.serialize( out, object.getAppId() );
        UUIDStreamSerializerUtils.serialize( out, object.getOrganizationId() );
    }

    @Override public AppConfigKey read( ObjectDataInput in ) throws IOException {
        UUID appId = UUIDStreamSerializerUtils.deserialize( in );
        UUID organizationId = UUIDStreamSerializerUtils.deserialize( in );
        return new AppConfigKey( appId, organizationId );
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.APP_CONFIG_KEY.ordinal();
    }

    @Override public void destroy() {

    }
}
