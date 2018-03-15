/*
 * Copyright (C) 2018. OpenLattice, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * You can contact the owner of the copyright at support@openlattice.com
 */

package com.openlattice.edm.internal;

import java.io.IOException;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.openlattice.data.serializers.FullQualifiedNameJacksonDeserializer;
import com.openlattice.data.serializers.FullQualifiedNameJacksonSerializer;
import com.openlattice.edm.type.EntityType;
import com.openlattice.mapstores.TestDataFactory;
import com.openlattice.serializer.AbstractJacksonSerializationTest;

public class EntityTypeSerializerTest extends AbstractJacksonSerializationTest<EntityType> {

    @BeforeClass
    public static void configureSerializer() {
        FullQualifiedNameJacksonSerializer.registerWithMapper( mapper );
        FullQualifiedNameJacksonDeserializer.registerWithMapper( mapper );
        FullQualifiedNameJacksonSerializer.registerWithMapper( smile );
        FullQualifiedNameJacksonDeserializer.registerWithMapper( smile );
    }

    @Override
    protected EntityType getSampleData() {
        return TestDataFactory.entityType();
    }

    @Override
    protected Class<EntityType> getClazz() {
        return EntityType.class;
    }

    @Test
    public void testIncludesCategory() throws IOException {
        String json = serialize( getSampleData() ).getJsonString();
        logger.debug( json );
        Assert.assertTrue( "Json must contain category property", json.contains( "category" ) );
    }
}