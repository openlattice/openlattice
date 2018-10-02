package com.openlattice.hazelcast.serializers;

import com.google.common.collect.LinkedHashMultimap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import com.openlattice.edm.requests.MetadataUpdate;
import com.openlattice.hazelcast.StreamSerializerTypeIds;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.springframework.stereotype.Component;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

@Component
public class MetadataUpdateStreamSerializer implements SelfRegisteringStreamSerializer<MetadataUpdate> {

    @Override
    public void write( ObjectDataOutput out, MetadataUpdate object ) throws IOException {
        serialize( out, object );
    }

    @Override
    public MetadataUpdate read( ObjectDataInput in ) throws IOException {
        return deserialize( in );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.METADATA_UPDATE.ordinal();
    }

    public static void serialize( ObjectDataOutput out, MetadataUpdate object ) throws IOException {
        OptionalStreamSerializers.serialize( out, object.getTitle(), DataOutput::writeUTF );
        OptionalStreamSerializers.serialize( out, object.getDescription(), DataOutput::writeUTF );
        OptionalStreamSerializers.serialize( out, object.getName(), DataOutput::writeUTF );
        OptionalStreamSerializers.serializeSet( out, object.getContacts(), DataOutput::writeUTF );
        OptionalStreamSerializers.serialize( out, object.getType(), FullQualifiedNameStreamSerializer::serialize );
        OptionalStreamSerializers.serialize( out, object.getPii(), DataOutput::writeBoolean );
        OptionalStreamSerializers.serialize( out, object.getDefaultShow(), DataOutput::writeBoolean );
        OptionalStreamSerializers.serialize( out, object.getUrl(), DataOutput::writeUTF );
        OptionalStreamSerializers.serialize( out, object.getPropertyTags(), ( o, pt ) -> {
            o.writeInt( pt.size() );
            for ( Map.Entry<UUID, String> e : pt.entries() ) {
                UUIDStreamSerializer.serialize( o, e.getKey() );
                o.writeUTF( e.getValue() );
            }
        } );
    }

    public static MetadataUpdate deserialize( ObjectDataInput in ) throws IOException {
        Optional<String> title = Optional.of( in.readUTF() );
        Optional<String> description = Optional.of( in.readUTF() );
        Optional<String> name = Optional.of( in.readUTF() );
        Optional<Set<String>> contacts = OptionalStreamSerializers.deserializeSet( in, DataInput::readUTF );
        Optional<FullQualifiedName> type = Optional.of( FullQualifiedNameStreamSerializer.deserialize( in ) );
        Optional<Boolean> pii = Optional.of( in.readBoolean() );
        Optional<Boolean> defaultShow = Optional.of( in.readBoolean() );
        Optional<String> url = Optional.of( in.readUTF() );
        int size = in.readInt();
        LinkedHashMultimap<UUID, String> pts = LinkedHashMultimap.create();
        for ( int i = 0; i < size; i++ ) {
            UUID uuid = UUIDStreamSerializer.deserialize( in );
            String str = in.readUTF();
            pts.put( uuid, str );
        }
        Optional<LinkedHashMultimap<UUID, String>> propertyTags = Optional.of( pts );
        return new MetadataUpdate( title, description, name, contacts, type, pii, defaultShow, url, propertyTags );
    }

    @Override
    public void destroy() {
    }

    @Override
    public Class<MetadataUpdate> getClazz() {
        return MetadataUpdate.class;
    }
}
