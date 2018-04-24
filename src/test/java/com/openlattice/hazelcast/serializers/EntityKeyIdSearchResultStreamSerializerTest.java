package com.openlattice.hazelcast.serializers;

import com.google.common.collect.ImmutableList;
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest;
import com.openlattice.search.requests.EntityKeyIdSearchResult;
import org.apache.commons.lang3.RandomUtils;

import java.io.Serializable;
import java.util.UUID;

public class EntityKeyIdSearchResultStreamSerializerTest extends
        AbstractStreamSerializerTest<EntityKeyIdSearchResultStreamSerializer, EntityKeyIdSearchResult> implements
        Serializable {
    @Override protected EntityKeyIdSearchResultStreamSerializer createSerializer() {
        return new EntityKeyIdSearchResultStreamSerializer();
    }

    @Override protected EntityKeyIdSearchResult createInput() {
        return new EntityKeyIdSearchResult( RandomUtils.nextLong( 0, 99999 ),
                ImmutableList.of( UUID.randomUUID(),
                        UUID.randomUUID(),
                        UUID.randomUUID(),
                        UUID.randomUUID(),
                        UUID.randomUUID() ) );
    }
}
