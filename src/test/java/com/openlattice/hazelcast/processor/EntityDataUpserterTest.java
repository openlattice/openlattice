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
import java.util.HashMap;
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
        Assert.assertEquals( 1, propertyMetadata.get( value ).getVersion()  );
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

        Map<UUID, Map<Object, PropertyMetadata>> mockMap = new HashMap<>();
        mockMap.put( propertyTypeId,
                Maps.newHashMap( ImmutableMap.of(
                        value,
                        new PropertyMetadata( 0,
                                Lists.newArrayList( 0L ),
                                lastWrite.minus( 1, ChronoUnit.DAYS ) ) ) ) );
        mockMap.put( propertyTypeId2,
                Maps.newHashMap( ImmutableMap.of(
                        value2,
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
        Assert.assertEquals( 0, propertyMetadata.get( value2 ).getVersion() );
        Assert.assertEquals( Lists.newArrayList( metadata.getVersion() ),
                propertyMetadata.get( value2 ).getVersions() );
        Assert.assertEquals( lastWrite.minus( 1, ChronoUnit.DAYS ), propertyMetadata.get( value2 ).getLastWrite() );
    }

    @Test
    public void createNewPropertyOnNonEmptyEntity() {
        EntityDataKey edk = new EntityDataKey( UUID.randomUUID(), UUID.randomUUID() );
        EntityDataMetadata metadata = new EntityDataMetadata(
                0,
                OffsetDateTime.now().minus( 1, ChronoUnit.DAYS ),
                OffsetDateTime.MIN );

        OffsetDateTime lastWrite = OffsetDateTime.now();
        SetMultimap<UUID, Object> properties = HashMultimap.create();
        UUID propertyTypeId = UUID.randomUUID();
        Object value = 1L;

        properties.put( propertyTypeId, value );
        Map<UUID, Map<Object, PropertyMetadata>> mockMap = new HashMap<>();
        mockMap.put( propertyTypeId,
                Maps.newHashMap( ImmutableMap.of( propertyTypeId,
                        new PropertyMetadata( 0,
                                Lists.newArrayList( 0L ),
                                lastWrite.minus( 1, ChronoUnit.DAYS ) ) ) ) );

        EntityDataValue edv = new EntityDataValue( metadata, mockMap );
        MockEntry mockEntry = new MockEntry( edk, edv );

        UUID propertyTypeId2 = UUID.randomUUID();
        Object value2 = 2L;

        properties.put( propertyTypeId2, value2 );

        EntityDataUpserter upserter = new EntityDataUpserter( properties, lastWrite );

        upserter.process( mockEntry );

        Map<UUID, Map<Object, PropertyMetadata>> mockProperties = mockEntry.getValue().getProperties();
        Assert.assertTrue( mockProperties.containsKey( propertyTypeId ) );
        Assert.assertTrue( mockProperties.containsKey( propertyTypeId2 ) );

        Map<Object, PropertyMetadata> propertyMetadata = mockProperties.get( propertyTypeId );

        Assert.assertTrue( propertyMetadata.containsKey( value ) );
        Assert.assertEquals( 1, propertyMetadata.get( value ).getVersion() );
        Assert.assertEquals( Lists.newArrayList( metadata.getVersion() + 1 ),
                propertyMetadata.get( value ).getVersions() );
        Assert.assertEquals( lastWrite, propertyMetadata.get( value ).getLastWrite() );

        propertyMetadata = mockProperties.get( propertyTypeId2 );

        Assert.assertTrue( propertyMetadata.containsKey( value2 ) );
        Assert.assertEquals( 1, propertyMetadata.get( value2 ).getVersion() );
        Assert.assertEquals( Lists.newArrayList( metadata.getVersion() + 1 ),
                propertyMetadata.get( value2 ).getVersions() );
        Assert.assertEquals( lastWrite, propertyMetadata.get( value2 ).getLastWrite() );
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

        MockEntry mockEntry = new MockEntry( edk, null );

        upserter.process( mockEntry );

        Map<UUID, Map<Object, PropertyMetadata>> mockProperties = mockEntry.getValue().getProperties();
        Assert.assertTrue( mockProperties.containsKey( propertyTypeId ) );
        Map<Object, PropertyMetadata> propertyMetadata = mockProperties.get( propertyTypeId );
        Assert.assertTrue( propertyMetadata.containsKey( value ) );
        Assert.assertEquals( 0, propertyMetadata.get( 1L ).getVersion() );
        Assert.assertEquals( Lists.newArrayList( 0L ), propertyMetadata.get( 1L ).getVersions() );
        Assert.assertEquals( lastWrite, propertyMetadata.get( 1L ).getLastWrite() );
    }

    private class MockEntry implements Entry<EntityDataKey, EntityDataValue> {
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
