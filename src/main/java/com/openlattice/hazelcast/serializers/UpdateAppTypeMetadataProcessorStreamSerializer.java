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
import com.openlattice.apps.processors.UpdateAppTypeMetadataProcessor;
import com.openlattice.edm.requests.MetadataUpdate;
import java.io.IOException;
import java.util.Optional;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.springframework.stereotype.Component;

@Component
public class UpdateAppTypeMetadataProcessorStreamSerializer implements SelfRegisteringStreamSerializer<UpdateAppTypeMetadataProcessor> {
    @Override public Class<? extends UpdateAppTypeMetadataProcessor> getClazz() {
        return UpdateAppTypeMetadataProcessor.class;
    }

    @Override public void write(
            ObjectDataOutput out, UpdateAppTypeMetadataProcessor object ) throws IOException {
        MetadataUpdate update = object.getUpdate();
        OptionalStreamSerializers.serialize( out, update.getTitle(), ObjectDataOutput::writeUTF );
        OptionalStreamSerializers.serialize( out, update.getDescription(), ObjectDataOutput::writeUTF );
        OptionalStreamSerializers.serialize( out, update.getType(), FullQualifiedNameStreamSerializer::serialize );
    }

    @Override public UpdateAppTypeMetadataProcessor read( ObjectDataInput in ) throws IOException {
        Optional<String> title = OptionalStreamSerializers.deserialize( in, ObjectDataInput::readUTF );
        Optional<String> description = OptionalStreamSerializers.deserialize( in, ObjectDataInput::readUTF );
        Optional<FullQualifiedName> type = OptionalStreamSerializers.deserialize( in,
                FullQualifiedNameStreamSerializer::deserialize );

        MetadataUpdate update = new MetadataUpdate(
                title,
                description,
                Optional.empty(),
                Optional.empty(),
                type,
                Optional.empty(),
                Optional.empty(),
                Optional.empty() );
        return new UpdateAppTypeMetadataProcessor( update );
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.UPDATE_APP_TYPE_METADATA_PROCESSOR.ordinal();
    }

    @Override public void destroy() {

    }
}
