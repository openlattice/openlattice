package com.openlattice.hazelcast.serializers;

import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest;
import com.openlattice.edm.requests.MetadataUpdate;
import com.openlattice.mapstores.TestDataFactory;

import java.io.Serializable;

public class MetadataUpdateStreamSerializerTest
        extends AbstractStreamSerializerTest<MetadataUpdateStreamSerializer, MetadataUpdate> implements
        Serializable {
    @Override
    protected MetadataUpdate createInput() {
        return TestDataFactory.metadataUpdate();
    }

    @Override
    protected MetadataUpdateStreamSerializer createSerializer() {
        return new MetadataUpdateStreamSerializer();
    }
}
