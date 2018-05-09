

/*
 * Copyright (C) 2018. OpenLattice, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@openlattice.com
 *
 */

package com.openlattice.hazelcast.serializers;

import static com.openlattice.hazelcast.processor.EntityDataUpserterTest.addProperty;
import static com.openlattice.hazelcast.processor.EntityDataUpserterTest.newMockEntry;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest;
import com.openlattice.data.EntityDataValue;
import com.openlattice.data.PropertyMetadata;
import com.openlattice.hazelcast.processor.EntityDataUpserterTest.MockEntry;
import com.openlattice.hazelcast.processors.EntityDataUpserter;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.UUID;
import org.junit.Assert;

public class EntityDataValueStreamSerializerTest
        extends AbstractStreamSerializerTest<EntityDataValueStreamSerializer, EntityDataValue>
        implements Serializable {
    private static final long serialVersionUID = 8869472746330274551L;

    @Override
    protected EntityDataValue createInput() {

        final OffsetDateTime lastWrite = OffsetDateTime.now();
        final MockEntry mockEntry = newMockEntry( lastWrite );
        final EntityDataValue edv = mockEntry.getValue();
        final OffsetDateTime preLastWrite = edv.getLastWrite();

        SetMultimap<UUID, Object> properties = HashMultimap.create();

        Object value = 1L;
        Object value2 = LocalDateTime.now();
        Object value3 = OffsetDateTime.now();
        Object value4 = "for the horde!";
        Object value5 = 12;
        Object value6 = 13L;
        Object value7 = "12345678123456781234567812345678";
        Object value8 = "123456781234567812345678123456781234567812345678123456781234567";
        Object value9 = "1234567812345678123456781234567812345678123456781234567812345678";
        Object value10 = "1234567812345678123456781234567812345678123456781234567812345678123456781234567812345678123456781234567812345678123456781234567812345678123456781234567812345678123456781234567812345678123456781234567812345678123456781234567812345678123456781234567812345678";

        final Map<UUID, Map<Object, PropertyMetadata>> mockMap = edv.getProperties();
        final UUID propertyTypeId = addProperty( value, mockMap, preLastWrite );
        final UUID propertyTypeId2 = addProperty( value2, mockMap, preLastWrite );
        final UUID propertyTypeId3 = UUID.randomUUID();
        addProperty( value4, mockMap, preLastWrite );
        addProperty( value5, mockMap, preLastWrite );
        addProperty( value6, mockMap, preLastWrite );
        addProperty( value7, mockMap, preLastWrite );
        addProperty( value8, mockMap, preLastWrite );
        addProperty( value9, mockMap, preLastWrite );
        addProperty( value10, mockMap, preLastWrite );
        addProperty( LocalTime.now(), mockMap, preLastWrite );
        addProperty( LocalTime.now(), mockMap, preLastWrite );
        addProperty( OffsetDateTime.now(), mockMap, preLastWrite );

        properties.put( propertyTypeId3, value3 );

        EntityDataUpserter upserter = new EntityDataUpserter( properties, lastWrite );

        //Ensure that expected properties are present / not present.
        Map<UUID, Map<Object, PropertyMetadata>> mockProperties = mockEntry.getValue().getProperties();

        Assert.assertTrue( mockProperties.containsKey( propertyTypeId ) );
        Assert.assertTrue( mockProperties.containsKey( propertyTypeId2 ) );
        Assert.assertFalse( mockProperties.containsKey( propertyTypeId3 ) );

        upserter.process( mockEntry );

        Assert.assertTrue( mockProperties.containsKey( propertyTypeId3 ) );

        return mockEntry.getValue();
    }

    @Override
    protected EntityDataValueStreamSerializer createSerializer() {
        return new EntityDataValueStreamSerializer();
    }

}
