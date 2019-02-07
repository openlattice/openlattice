package com.openlattice.hazelcast.serializers;

import com.google.common.collect.LinkedHashMultimap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.hazelcast.serializers.GuavaStreamSerializersKt;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import com.openlattice.edm.requests.MetadataUpdate;
import com.openlattice.hazelcast.StreamSerializerTypeIds;
import java.io.DataInput;
import java.io.IOException;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.springframework.stereotype.Component;

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

    @Override
    public void destroy() {
    }

    @Override
    public Class<MetadataUpdate> getClazz() {
        return MetadataUpdate.class;
    }

    public static void serialize( ObjectDataOutput out, MetadataUpdate object ) throws IOException {
        OptionalStreamSerializers.serialize( out, object.getTitle(), ObjectDataOutput::writeUTF );
        OptionalStreamSerializers.serialize( out, object.getDescription(), ObjectDataOutput::writeUTF );
        OptionalStreamSerializers.serialize( out, object.getName(), ObjectDataOutput::writeUTF );
        OptionalStreamSerializers.serializeSet( out, object.getContacts(), ObjectDataOutput::writeUTF );
        OptionalStreamSerializers.serialize( out, object.getType(), FullQualifiedNameStreamSerializer::serialize );
        OptionalStreamSerializers.serialize( out, object.getPii(), ObjectDataOutput::writeBoolean );
        OptionalStreamSerializers.serialize( out, object.getDefaultShow(), ObjectDataOutput::writeBoolean );
        OptionalStreamSerializers.serialize( out, object.getUrl(), ObjectDataOutput::writeUTF );
        OptionalStreamSerializers
                .serialize( out, object.getPropertyTags(), GuavaStreamSerializersKt::serializeSetMultimap );
        OptionalStreamSerializers.serialize( out, object.getOrganizationId(), UUIDStreamSerializer::serialize );
    }

    public static MetadataUpdate deserialize( ObjectDataInput in ) throws IOException {
        Optional<String> title = OptionalStreamSerializers.deserialize( in, ObjectDataInput::readUTF );
        Optional<String> description = OptionalStreamSerializers.deserialize( in, ObjectDataInput::readUTF );
        Optional<String> name = OptionalStreamSerializers.deserialize( in, ObjectDataInput::readUTF );
        Optional<Set<String>> contacts = OptionalStreamSerializers.deserializeSet( in, DataInput::readUTF );
        Optional<FullQualifiedName> type = OptionalStreamSerializers
                .deserialize( in, FullQualifiedNameStreamSerializer::deserialize );
        Optional<Boolean> pii = OptionalStreamSerializers.deserialize( in, ObjectDataInput::readBoolean );
        Optional<Boolean> defaultShow = OptionalStreamSerializers.deserialize( in, ObjectDataInput::readBoolean );
        Optional<String> url = OptionalStreamSerializers.deserialize( in, ObjectDataInput::readUTF );
        Optional<LinkedHashMultimap<UUID, String>> propertyTags = OptionalStreamSerializers
                .deserialize( in, GuavaStreamSerializersKt::deserializeLinkedHashMultimap );
        Optional<UUID> organizationId = OptionalStreamSerializers
                .deserialize( in, UUIDStreamSerializer::deserialize );
        return new MetadataUpdate( title,
                description,
                name,
                contacts,
                type,
                pii,
                defaultShow,
                url,
                propertyTags,
                organizationId);
    }
}
