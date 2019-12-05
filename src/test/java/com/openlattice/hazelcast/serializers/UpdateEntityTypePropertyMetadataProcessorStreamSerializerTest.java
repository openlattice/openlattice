package com.openlattice.hazelcast.serializers;

import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest;
import com.openlattice.edm.requests.MetadataUpdate;
import com.openlattice.edm.types.processors.UpdateEntityTypePropertyMetadataProcessor;

import java.io.Serializable;
import java.util.Optional;

public class UpdateEntityTypePropertyMetadataProcessorStreamSerializerTest extends
        AbstractStreamSerializerTest<UpdateEntityTypePropertyMetadataProcessorStreamSerializer, UpdateEntityTypePropertyMetadataProcessor>
        implements Serializable {
    private static final long serialVersionUID = -5379472664347656668L;

    @Override
    protected UpdateEntityTypePropertyMetadataProcessorStreamSerializer createSerializer() {
        return new UpdateEntityTypePropertyMetadataProcessorStreamSerializer();
    }

    @Override
    protected UpdateEntityTypePropertyMetadataProcessor createInput() {
        MetadataUpdate update = new MetadataUpdate(
                Optional.of( "title" ),
                Optional.of( "description" ),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
        return new UpdateEntityTypePropertyMetadataProcessor( update );
    }

}
