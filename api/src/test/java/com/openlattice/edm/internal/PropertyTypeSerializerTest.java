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

import com.openlattice.edm.type.PropertyType;
import com.openlattice.serializer.AbstractJacksonSerializationTest;
import org.junit.BeforeClass;

import com.openlattice.data.serializers.FullQualifiedNameJacksonDeserializer;
import com.openlattice.data.serializers.FullQualifiedNameJacksonSerializer;
import com.openlattice.mapstores.TestDataFactory;

public class PropertyTypeSerializerTest extends AbstractJacksonSerializationTest<PropertyType> {
    @BeforeClass
    public static void configureSerializer() {
        FullQualifiedNameJacksonSerializer.registerWithMapper( mapper );

        FullQualifiedNameJacksonSerializer.registerWithMapper( smile );

    }

    @Override
    protected PropertyType getSampleData() {
        return TestDataFactory.propertyType();
    }

    @Override
    protected Class<PropertyType> getClazz() {
        return PropertyType.class;
    }
}
