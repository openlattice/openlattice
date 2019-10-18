package com.openlattice.data.requests;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Stopwatch;
import com.openlattice.data.EntityKey;
import com.openlattice.serializer.AbstractJacksonSerializationTest;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class EntityKeyJacksonSerializationTest extends AbstractJacksonSerializationTest<EntityKey> {
    @Override protected EntityKey getSampleData() throws IOException {
        return new EntityKey( UUID.randomUUID(), RandomStringUtils.random( 64 ) );
    }

    @Override protected Class<EntityKey> getClazz() {
        return EntityKey.class;
    }

    @Test
    @Ignore
    public void timing() throws IOException {
        final int count = 100000;
        final List<EntityKey> entityKeys = new ArrayList<>( count );
        for ( int i = 0; i < count; ++i ) {
            entityKeys.add( getSampleData() );
        }

        final var sw = Stopwatch.createStarted();

        final var serdesKeys = mapper
                .readValue(
                        mapper.writeValueAsString( entityKeys ),
                        new TypeReference<List<EntityKey>>() {
                        } );
        logger.info("Serialization overhead for {} keys is {} ms", count, sw.elapsed( TimeUnit.MILLISECONDS ));

        Assert.assertEquals(entityKeys, serdesKeys);
    }
}
