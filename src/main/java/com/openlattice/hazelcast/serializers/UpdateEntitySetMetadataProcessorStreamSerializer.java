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
import com.openlattice.edm.requests.MetadataUpdate;
import com.openlattice.edm.types.processors.UpdateEntitySetMetadataProcessor;
import java.io.IOException;
import java.util.Optional;
import java.util.Set;
import org.springframework.stereotype.Component;

@Component
public class UpdateEntitySetMetadataProcessorStreamSerializer
        implements SelfRegisteringStreamSerializer<UpdateEntitySetMetadataProcessor> {

    @Override
    public void write( ObjectDataOutput out, UpdateEntitySetMetadataProcessor object ) throws IOException {
        MetadataUpdate update = object.getUpdate();
        OptionalStreamSerializers.serialize( out, update.getTitle(), ObjectDataOutput::writeUTF );
        OptionalStreamSerializers.serialize( out, update.getDescription(), ObjectDataOutput::writeUTF );
        OptionalStreamSerializers.serialize( out, update.getName(), ObjectDataOutput::writeUTF );
        OptionalStreamSerializers.serializeSet( out, update.getContacts(), ObjectDataOutput::writeUTF );
    }

    @Override
    public UpdateEntitySetMetadataProcessor read( ObjectDataInput in ) throws IOException {
        Optional<String> title = OptionalStreamSerializers.deserialize( in, ObjectDataInput::readUTF );
        Optional<String> description = OptionalStreamSerializers.deserialize( in, ObjectDataInput::readUTF );
        Optional<String> name = OptionalStreamSerializers.deserialize( in, ObjectDataInput::readUTF );
        Optional<Set<String>> contacts = OptionalStreamSerializers.deserializeSet( in, ObjectDataInput::readUTF );

        MetadataUpdate update = new MetadataUpdate(
                title,
                description,
                name,
                contacts,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty() );
        return new UpdateEntitySetMetadataProcessor( update );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.UPDATE_ENTITY_SET_METADATA_PROCESSOR.ordinal();
    }

    @Override
    public void destroy() {}

    @Override
    public Class<UpdateEntitySetMetadataProcessor> getClazz() {
        return UpdateEntitySetMetadataProcessor.class;
    }
}