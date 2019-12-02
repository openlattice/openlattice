package com.openlattice.hazelcast.serializers;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializers;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import com.openlattice.edm.set.EntitySetPropertyMetadata;
import com.openlattice.edm.type.EntityTypePropertyMetadata;
import com.openlattice.hazelcast.StreamSerializerTypeIds;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.LinkedHashSet;

@Component
public class EntityTypePropertyMetadataStreamSerializer
        implements SelfRegisteringStreamSerializer<EntityTypePropertyMetadata> {

    @Override
    public void write( ObjectDataOutput out, EntityTypePropertyMetadata object ) throws IOException {
        out.writeUTF( object.getTitle() );
        out.writeUTF( object.getDescription() );
        out.writeObject (object.getTags() );
        out.writeBoolean( object.getDefaultShow() );
    }

    @Override
    public EntityTypePropertyMetadata read( ObjectDataInput in ) throws IOException {
        String title = in.readUTF();
        String description = in.readUTF();
        LinkedHashSet tags = in.readObject();
        Boolean defaultShow = in.readBoolean();

        return new EntityTypePropertyMetadata( title, description, tags, defaultShow );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.ENTITY_TYPE_PROPERTY_METADATA.ordinal();
    }

    @Override
    public void destroy() {}

    @Override
    public Class<? extends EntityTypePropertyMetadata> getClazz() {
        return EntityTypePropertyMetadata.class;
    }

}
