package com.openlattice.hazelcast.serializers;

import com.google.common.collect.ImmutableSet;
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest;
import com.openlattice.mapstores.TestDataFactory;
import com.openlattice.search.requests.EntityDataKeySearchResult;
import org.apache.commons.lang3.RandomUtils;

import java.io.Serializable;

public class EntityDataKeySearchResultStreamSerializerTest extends
        AbstractStreamSerializerTest<EntityDataKeySearchResultStreamSerializer, EntityDataKeySearchResult> implements
        Serializable {
    @Override protected EntityDataKeySearchResultStreamSerializer createSerializer() {
        return new EntityDataKeySearchResultStreamSerializer();
    }

    @Override protected EntityDataKeySearchResult createInput() {
        return new EntityDataKeySearchResult( RandomUtils.nextLong( 0, 99999 ),
                ImmutableSet.of( TestDataFactory.entityDataKey(),
                        TestDataFactory.entityDataKey(),
                        TestDataFactory.entityDataKey(),
                        TestDataFactory.entityDataKey(),
                        TestDataFactory.entityDataKey(),
                        TestDataFactory.entityDataKey() ) );
    }
}
