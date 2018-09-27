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

import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializer;
import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializers;
import com.openlattice.hazelcast.StreamSerializerTypeIds;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import com.openlattice.edm.set.EntitySetPropertyMetadata;
import java.io.IOException;
import org.springframework.stereotype.Component;

@Component
public class EntitySetPropertyMetadataStreamSerializer
        implements SelfRegisteringStreamSerializer<EntitySetPropertyMetadata> {

    @Override
    public void write( ObjectDataOutput out, EntitySetPropertyMetadata object ) throws IOException {
        out.writeUTF( object.getTitle() );
        out.writeUTF( object.getDescription() );
        SetStreamSerializers.fastStringSetSerialize( out, object.getTags() );
        out.writeBoolean( object.getDefaultShow() );
    }

    @Override
    public EntitySetPropertyMetadata read( ObjectDataInput in ) throws IOException {
        String title = in.readUTF();
        String description = in.readUTF();
        var tags = SetStreamSerializers.orderedFastStringSetDeserialize( in );
        boolean defaultShow = in.readBoolean();
        return new EntitySetPropertyMetadata( title, description, tags, defaultShow );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.ENTITY_SET_PROPERTY_METADATA.ordinal();
    }

    @Override
    public void destroy() {}

    @Override
    public Class<? extends EntitySetPropertyMetadata> getClazz() {
        return EntitySetPropertyMetadata.class;
    }

}
