package com.openlattice.hazelcast.serializers;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import com.openlattice.edm.types.processors.UpdateEntitySetPropertyMetadataProcessor;
import com.openlattice.edm.types.processors.UpdateEntityTypePropertyMetadataProcessor;
import com.openlattice.hazelcast.StreamSerializerTypeIds;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class UpdateEntityTypePropertyMetadataProcessorStreamSerializer        implements
        SelfRegisteringStreamSerializer<UpdateEntityTypePropertyMetadataProcessor> {

    @Override
    public void write( ObjectDataOutput out, UpdateEntityTypePropertyMetadataProcessor object ) throws IOException {
        MetadataUpdateStreamSerializer.serialize( out, object.getUpdate() );
    }

    @Override
    public UpdateEntityTypePropertyMetadataProcessor read( ObjectDataInput in ) throws IOException {

        return new UpdateEntityTypePropertyMetadataProcessor( MetadataUpdateStreamSerializer.deserialize( in ) );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.UPDATE_ENTITY_TYPE_PROPERTY_METADATA_PROCESSOR.ordinal();
    }

    @Override
    public void destroy() {
    }

    @Override
    public Class<? extends UpdateEntityTypePropertyMetadataProcessor> getClazz() {
        return UpdateEntityTypePropertyMetadataProcessor.class;
    }

}
