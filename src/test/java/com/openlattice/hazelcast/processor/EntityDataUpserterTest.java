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

package com.openlattice.hazelcast.processor;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;
import com.openlattice.data.EntityDataKey;
import com.openlattice.data.EntityDataMetadata;
import com.openlattice.data.EntityDataValue;
import com.openlattice.data.PropertyMetadata;
import com.openlattice.hazelcast.processors.EntityDataUpserter;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class EntityDataUpserterTest {

    @Test
    public void createNewPropertiesOnNonEmptyEntity() {
        EntityDataKey edk = new EntityDataKey( UUID.randomUUID(), UUID.randomUUID() );
        EntityDataMetadata metadata = new EntityDataMetadata(
                0,
                OffsetDateTime.now().minus( 1, ChronoUnit.DAYS ),
                OffsetDateTime.MIN );

        OffsetDateTime lastWrite = OffsetDateTime.now();
        SetMultimap<UUID, Object> properties = HashMultimap.create();
        UUID propertyTypeId = UUID.randomUUID();
        Object value = 1L;

        UUID propertyTypeId2 = UUID.randomUUID();
        Object value2 = 2L;

        properties.put( propertyTypeId, value );
        properties.put( propertyTypeId2, value2 );

        Map<UUID, Map<Object, PropertyMetadata>> mockMap = new HashMap<>();

        mockMap.put( propertyTypeId,
                Maps.newHashMap( ImmutableMap.of(
                        propertyTypeId,
                        new PropertyMetadata( 0,
                                Lists.newArrayList( 0L ),
                                lastWrite.minus( 1, ChronoUnit.DAYS ) ) ) ) );
        mockMap.put( propertyTypeId2,
                Maps.newHashMap( ImmutableMap.of(
                        propertyTypeId2,
                        new PropertyMetadata( 0,
                                Lists.newArrayList( 0L ),
                                lastWrite.minus( 1, ChronoUnit.DAYS ) ) ) ) );

        EntityDataValue edv = new EntityDataValue( metadata, mockMap );
        MockEntry mockEntry = new MockEntry( edk, edv );

        EntityDataUpserter upserter = new EntityDataUpserter( properties, lastWrite );

        upserter.process( mockEntry );

        Map<UUID, Map<Object, PropertyMetadata>> mockProperties = mockEntry.getValue().getProperties();
        Assert.assertTrue( mockProperties.containsKey( propertyTypeId ) );
        Assert.assertTrue( mockProperties.containsKey( propertyTypeId2 ) );

        Map<Object, PropertyMetadata> propertyMetadata = mockProperties.get( propertyTypeId );

        Assert.assertTrue( propertyMetadata.containsKey( value ) );
        Assert.assertEquals( 1, propertyMetadata.get( value ).getVersion() );
        Assert.assertEquals( Lists.newArrayList( metadata.getVersion() ),
                propertyMetadata.get( value ).getVersions() );
        Assert.assertEquals( lastWrite, propertyMetadata.get( value ).getLastWrite() );

        propertyMetadata = mockProperties.get( propertyTypeId2 );

        Assert.assertTrue( propertyMetadata.containsKey( value2 ) );
        Assert.assertEquals( 1, propertyMetadata.get( value2 ).getVersion() );
        Assert.assertEquals( Lists.newArrayList( metadata.getVersion() ),
                propertyMetadata.get( value2 ).getVersions() );
        Assert.assertEquals( lastWrite, propertyMetadata.get( value2 ).getLastWrite() );
    }

    @Test
    public void createOneNewPropertyOnEntityWithTwo() {
        EntityDataKey edk = new EntityDataKey( UUID.randomUUID(), UUID.randomUUID() );
        long version = 0;

        OffsetDateTime lastWrite = OffsetDateTime.now();
        OffsetDateTime preLastWrite = lastWrite.minus( 1, ChronoUnit.DAYS );

        EntityDataMetadata metadata = new EntityDataMetadata(
                version,
                preLastWrite,
                OffsetDateTime.MIN );

        SetMultimap<UUID, Object> properties = HashMultimap.create();
        Object value = 1L;
        Object value2 = 2L;
        Object value3 = 3L;

        Map<UUID, Map<Object, PropertyMetadata>> mockMap = new HashMap<>();
        UUID propertyTypeId = addProperty( value, mockMap, preLastWrite );
        UUID propertyTypeId2 = addProperty( value2, mockMap, preLastWrite );
        UUID propertyTypeId3 = UUID.randomUUID();

        //        properties.put( propertyTypeId, value );
        //        properties.put( propertyTypeId2, value2 );

        properties.put( propertyTypeId3, value3 );

        EntityDataValue edv = new EntityDataValue( metadata, mockMap );
        MockEntry mockEntry = new MockEntry( edk, edv );

        EntityDataUpserter upserter = new EntityDataUpserter( properties, lastWrite );

        //Ensure that expected properties are present / not present.
        Map<UUID, Map<Object, PropertyMetadata>> mockProperties = mockEntry.getValue().getProperties();

        Assert.assertTrue( mockProperties.containsKey( propertyTypeId ) );
        Assert.assertTrue( mockProperties.containsKey( propertyTypeId2 ) );
        Assert.assertFalse( mockProperties.containsKey( propertyTypeId3 ) );

        checkCorrectMetdata( mockProperties, propertyTypeId, value, version, Arrays.asList( version ), preLastWrite );

        upserter.process( mockEntry );

        //Ensure that expected properties are present.
        mockProperties = mockEntry.getValue().getProperties();
        Assert.assertTrue( mockProperties.containsKey( propertyTypeId ) );
        Assert.assertTrue( mockProperties.containsKey( propertyTypeId2 ) );
        Assert.assertTrue( mockProperties.containsKey( propertyTypeId3 ) );

        Map<Object, PropertyMetadata> propertyMetadata = mockProperties.get( propertyTypeId );

        checkCorrectMetdata( mockProperties, propertyTypeId, value, version, Arrays.asList( version ), preLastWrite );
        checkCorrectMetdata( mockProperties, propertyTypeId2, value2, version, Arrays.asList( version ), preLastWrite );
        checkCorrectMetdata( mockProperties,
                propertyTypeId3,
                value3,
                version + 1,
                Arrays.asList( version + 1 ),
                lastWrite );
    }

    @Test
    public void createNewPropertyOnNonEmptyEntity() {
        //Setup mock entry for testing.
        OffsetDateTime lastWrite = OffsetDateTime.now();
        MockEntry mockEntry = newMockEntry( lastWrite );
        EntityDataValue edv = mockEntry.getValue();
        long version = edv.getVersion();
        OffsetDateTime preLastWrite = edv.getLastWrite();

        SetMultimap<UUID, Object> properties = HashMultimap.create();
        Map<UUID, Map<Object, PropertyMetadata>> mockMap = edv.getProperties();

        final Object value = 1L;
        final Object value2 = 2L;

        final UUID propertyTypeId = addProperty( value, edv.getProperties(), preLastWrite );

        final UUID propertyTypeId2 = UUID.randomUUID();
        properties.put( propertyTypeId2, value2 );

        //Setup property that we will use to add non-empty entry.
        EntityDataUpserter upserter = new EntityDataUpserter( properties, lastWrite );
        final Map<UUID, Map<Object, PropertyMetadata>> mockProperties = mockEntry.getValue().getProperties();

        Assert.assertTrue( mockProperties.containsKey( propertyTypeId ) );
        Assert.assertFalse( mockProperties.containsKey( propertyTypeId2 ) );

        upserter.process( mockEntry );

        Assert.assertTrue( mockProperties.containsKey( propertyTypeId ) );
        Assert.assertTrue( mockProperties.containsKey( propertyTypeId2 ) );

        checkCorrectMetdata( mockProperties, propertyTypeId, value, version, Arrays.asList( version ), preLastWrite );
        checkCorrectMetdata( mockProperties, propertyTypeId2, value2, version+1, Arrays.asList( version+1 ), lastWrite );

//        Map<Object, PropertyMetadata> propertyMetadata = mockProperties.get( propertyTypeId );
//
//        //Check that first value inserted is there
//        Assert.assertTrue( propertyMetadata.containsKey( value ) );
//        //Check that version for first property has not increased
//        Assert.assertEquals( version + 1, propertyMetadata.get( value ).getVersion() );
//        //Check that metadata version list for first property is only the original version
//        Assert.assertEquals( Lists.newArrayList( version ),
//                propertyMetadata.get( value ).getVersions() );
//        //Check that last write is set correctly
//        Assert.assertEquals( lastWrite2, propertyMetadata.get( value ).getLastWrite() );
//
//        propertyMetadata = mockProperties.get( propertyTypeId2 );
//        //Check that second value is inserted
//        Assert.assertTrue( propertyMetadata.containsKey( value2 ) );
//        //Check that version for second property is version + 1
//        Assert.assertEquals( version + 1, propertyMetadata.get( value2 ).getVersion() );
//        //Check that second value is inserted with correct version numbers
//        Assert.assertEquals( Lists.newArrayList( version + 1 ),
//                propertyMetadata.get( value2 ).getVersions() );
//        //Check that last write is set correctly.
//        Assert.assertEquals( lastWrite2, propertyMetadata.get( value2 ).getLastWrite() );
    }

    @Test
    public void addProperties() {
        EntityDataKey edk = new EntityDataKey( UUID.randomUUID(), UUID.randomUUID() );
        SetMultimap<UUID, Object> properties = HashMultimap.create();
        UUID propertyTypeId = UUID.randomUUID();
        Object value = 1L;

        properties.put( propertyTypeId, value );

        OffsetDateTime lastWrite = OffsetDateTime.now();

        EntityDataUpserter upserter = new EntityDataUpserter( properties, lastWrite );

        //Create an empty mock entry.
        MockEntry mockEntry = new MockEntry( edk, null );

        upserter.process( mockEntry );

        Map<UUID, Map<Object, PropertyMetadata>> mockProperties = mockEntry.getValue().getProperties();
        checkCorrectMetdata( mockProperties, propertyTypeId,value, 0L, Arrays.asList(0L), lastWrite );
//        Assert.assertTrue( mockProperties.containsKey( propertyTypeId ) );
//        Map<Object, PropertyMetadata> propertyMetadata = mockProperties.get( propertyTypeId );
//        Assert.assertTrue( propertyMetadata.containsKey( value ) );
//        Assert.assertEquals( 0, propertyMetadata.get( 1L ).getVersion() );
//        Assert.assertEquals( Lists.newArrayList( 0L ), propertyMetadata.get( 1L ).getVersions() );
//        Assert.assertEquals( lastWrite, propertyMetadata.get( 1L ).getLastWrite() );
    }

    public static MockEntry newMockEntry( OffsetDateTime lastWrite ) {
        EntityDataKey edk = new EntityDataKey( UUID.randomUUID(), UUID.randomUUID() );
        long version = 0;

        OffsetDateTime preLastWrite = lastWrite.minus( 1, ChronoUnit.DAYS );

        EntityDataMetadata metadata = new EntityDataMetadata(
                version,
                preLastWrite,
                OffsetDateTime.MIN );

        EntityDataValue edv = new EntityDataValue( metadata, new HashMap<>() );
        return new MockEntry( edk, edv );
    }

    public static UUID addProperty(
            Object value,
            Map<UUID, Map<Object, PropertyMetadata>> m,
            OffsetDateTime writeTime ) {
        UUID propertyTypeId = UUID.randomUUID();
        PropertyMetadata
                pm = PropertyMetadata.newPropertyMetadata( 0, writeTime );
        m.put( propertyTypeId, Maps.newHashMap( ImmutableMap.of( value, pm ) ) );
        return propertyTypeId;
    }

    public static void checkCorrectMetdata(
            Map<UUID, Map<Object, PropertyMetadata>> mockProperties,
            UUID propertyTypeId,
            Object value,
            long version,
            List<Long> versions,
            OffsetDateTime lastWrite ) {
        Map<Object, PropertyMetadata> propertyMetadata = mockProperties.get( propertyTypeId );
        Assert.assertTrue( propertyMetadata.containsKey( value ) );
        Assert.assertEquals( version, propertyMetadata.get( value ).getVersion() );
        Assert.assertEquals( versions, propertyMetadata.get( value ).getVersions() );
        Assert.assertEquals( lastWrite, propertyMetadata.get( value ).getLastWrite() );
    }

    private static class MockEntry implements Entry<EntityDataKey, EntityDataValue> {
        private final EntityDataKey   edk;
        private       EntityDataValue edv;

        private MockEntry( EntityDataKey edk, EntityDataValue edv ) {
            this.edk = edk;
            this.edv = edv;
        }

        @Override public EntityDataKey getKey() {
            return edk;
        }

        @Override public EntityDataValue getValue() {
            return edv;
        }

        @Override public EntityDataValue setValue( EntityDataValue value ) {
            try {
                return edv;
            } finally {
                edv = value;
            }
        }
    }
}
