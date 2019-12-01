package com.openlattice.hazelcast.serializers;

import com.google.common.collect.Sets;
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest;
import com.openlattice.edm.type.EntityTypePropertyMetadata;
import org.apache.commons.lang3.RandomStringUtils;

import java.io.Serializable;
import java.util.Arrays;
import java.util.LinkedHashSet;
import org.apache.commons.lang3.RandomUtils;

import static org.junit.Assert.*;

public class EntityTypePropertyMetadataStreamSerializerTest
        extends AbstractStreamSerializerTest<EntityTypePropertyMetadataStreamSerializer, EntityTypePropertyMetadata>
        implements Serializable {
    private static final long serialVersionUID = 5114029297563838101L;

    @Override
    protected EntityTypePropertyMetadataStreamSerializer createSerializer() {
        return new EntityTypePropertyMetadataStreamSerializer();
    }

    @Override
    protected EntityTypePropertyMetadata createInput() {
        return new EntityTypePropertyMetadata(
                "title",
                "description",
                Sets.newLinkedHashSet( Arrays.asList( "foo", "bar" ) ),
                RandomUtils.nextBoolean() );
    }

}
