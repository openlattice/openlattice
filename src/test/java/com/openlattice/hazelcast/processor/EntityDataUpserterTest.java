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

import static com.openlattice.data.PropertyMetadata.hashObject;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;
import com.openlattice.data.EntityDataKey;
import com.openlattice.data.EntityDataMetadata;
import com.openlattice.data.EntityDataValue;
import com.openlattice.data.PropertyMetadata;
import com.openlattice.hazelcast.processors.EntityDataUpserter;
import com.openlattice.hazelcast.processors.MergeFinalizer;
import com.openlattice.hazelcast.processors.SyncFinalizer;
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
 * Ensures that the logic of the {@link EntityDataUpserter} works correctly.
 */
public class EntityDataUpserterTest {

    @Test
    public void overwritePropertiesOnNonEmptyEntityAndSync() {
        final OffsetDateTime lastWrite = OffsetDateTime.now();
        final MockEntry mockEntry = newMockEntry( lastWrite );
        final EntityDataValue edv = mockEntry.getValue();
        final long version = edv.getVersion();
        final OffsetDateTime preLastWrite = edv.getLastWrite();

        SetMultimap<UUID, Object> properties = HashMultimap.create();
        Object value = 1L;
        Object value2 = 2L;

        UUID propertyTypeId = addProperty( value, edv.getProperties(), preLastWrite );
        UUID propertyTypeId2 = addProperty( value2, edv.getProperties(), preLastWrite );

        properties.put( propertyTypeId, value );
        properties.put( propertyTypeId2, value2 );

        EntityDataUpserter upserter = new EntityDataUpserter( properties, lastWrite );

        Map<UUID, Map<Object, PropertyMetadata>> mockProperties = mockEntry.getValue().getProperties();

        Assert.assertTrue( mockProperties.containsKey( propertyTypeId ) );
        Assert.assertTrue( mockProperties.containsKey( propertyTypeId2 ) );

        upserter.process( mockEntry );

        Assert.assertTrue( mockProperties.containsKey( propertyTypeId ) );
        Assert.assertTrue( mockProperties.containsKey( propertyTypeId2 ) );

        checkCorrectMetdata( mockProperties, propertyTypeId, value, version, Arrays.asList( version ), preLastWrite );
        checkCorrectMetdata( mockProperties, propertyTypeId2, value2, version, Arrays.asList( version ), preLastWrite );

        SyncFinalizer syncFinalizer = new SyncFinalizer( preLastWrite );

        syncFinalizer.process( mockEntry );

        final long syncVersion = mockEntry.getValue().getProperties().get( propertyTypeId ).get( value ).getVersion();
        final long syncVersion2 = mockEntry.getValue().getProperties().get( propertyTypeId2 ).get( value2 )
                .getVersion();

        Assert.assertEquals( version, syncVersion );
        Assert.assertEquals( version, syncVersion2 );

        //Since it wasn't a delete the last write timestamp doesn't need to be updated.
        checkCorrectMetdata( mockProperties, propertyTypeId, value, version, Arrays.asList( version ), preLastWrite );
        checkCorrectMetdata( mockProperties, propertyTypeId2, value2, version, Arrays.asList( version ), preLastWrite );

        Assert.assertTrue( mockEntry.getValue().getVersion() >= preLastWrite.toInstant().toEpochMilli() );
    }

    @Test
    public void overwritePropertiesOnNonEmptyEntityAndMerge() {
        final OffsetDateTime lastWrite = OffsetDateTime.now();
        final MockEntry mockEntry = newMockEntry( lastWrite );
        final EntityDataValue edv = mockEntry.getValue();
        final long version = edv.getVersion();
        final OffsetDateTime preLastWrite = edv.getLastWrite();

        SetMultimap<UUID, Object> properties = HashMultimap.create();
        Object value = 1L;
        Object value2 = 2L;

        UUID propertyTypeId = addProperty( value, edv.getProperties(), preLastWrite );
        UUID propertyTypeId2 = addProperty( value2, edv.getProperties(), preLastWrite );

        properties.put( propertyTypeId, value );
        properties.put( propertyTypeId2, value2 );

        EntityDataUpserter upserter = new EntityDataUpserter( properties, lastWrite );

        Map<UUID, Map<Object, PropertyMetadata>> mockProperties = mockEntry.getValue().getProperties();

        Assert.assertTrue( mockProperties.containsKey( propertyTypeId ) );
        Assert.assertTrue( mockProperties.containsKey( propertyTypeId2 ) );

        upserter.process( mockEntry );

        Assert.assertTrue( mockProperties.containsKey( propertyTypeId ) );
        Assert.assertTrue( mockProperties.containsKey( propertyTypeId2 ) );

        checkCorrectMetdata( mockProperties, propertyTypeId, value, version, Arrays.asList( version ), preLastWrite );
        checkCorrectMetdata( mockProperties, propertyTypeId2, value2, version, Arrays.asList( version ), preLastWrite );

        OffsetDateTime entityWrite = OffsetDateTime.now().plus( 1, ChronoUnit.SECONDS );
        MergeFinalizer mergeFinalizer = new MergeFinalizer( entityWrite );

        mergeFinalizer.process( mockEntry );

        Assert.assertEquals( mockEntry.getValue().getVersion(), entityWrite.toInstant().toEpochMilli() );
    }

    @Test
    public void createOneNewPropertyOnEntityWithTwo() {
        final OffsetDateTime lastWrite = OffsetDateTime.now();
        final MockEntry mockEntry = newMockEntry( lastWrite );
        final EntityDataValue edv = mockEntry.getValue();
        final long version = edv.getVersion();
        final OffsetDateTime preLastWrite = edv.getLastWrite();

        SetMultimap<UUID, Object> properties = HashMultimap.create();

        Object value = 1L;
        Object value2 = 2L;
        Object value3 = 3L;

        final Map<UUID, Map<Object, PropertyMetadata>> mockMap = edv.getProperties();
        final UUID propertyTypeId = addProperty( value, mockMap, preLastWrite );
        final UUID propertyTypeId2 = addProperty( value2, mockMap, preLastWrite );
        final UUID propertyTypeId3 = UUID.randomUUID();

        properties.put( propertyTypeId3, value3 );

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

        final long expectedVersion = lastWrite.toInstant().toEpochMilli();

        checkCorrectMetdata( mockProperties, propertyTypeId, value, version, Arrays.asList( version ), preLastWrite );
        checkCorrectMetdata( mockProperties, propertyTypeId2, value2, version, Arrays.asList( version ), preLastWrite );
        checkCorrectMetdata( mockProperties,
                propertyTypeId3,
                value3,
                expectedVersion,
                Arrays.asList( expectedVersion ),
                lastWrite );
    }

    @Test
    public void createNewPropertyOnNonEmptyEntityAndMergeFinalize() {
        //Setup mock entry for testing.
        OffsetDateTime lastWrite = OffsetDateTime.now();
        MockEntry mockEntry = newMockEntry( lastWrite );
        EntityDataValue edv = mockEntry.getValue();
        long version = edv.getVersion();
        OffsetDateTime preLastWrite = edv.getLastWrite();

        SetMultimap<UUID, Object> properties = HashMultimap.create();

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
        checkCorrectMetdata( mockProperties,
                propertyTypeId2,
                value2,
                lastWrite.toInstant().toEpochMilli(),
                Arrays.asList( lastWrite.toInstant().toEpochMilli() ),
                lastWrite );

        OffsetDateTime entityWrite = OffsetDateTime.now().plus( 1, ChronoUnit.SECONDS );
        MergeFinalizer mergeFinalizer = new MergeFinalizer( entityWrite );

        mergeFinalizer.process( mockEntry );

        Assert.assertEquals( mockEntry.getValue().getVersion(), entityWrite.toInstant().toEpochMilli() );
    }

    @Test
    public void createNewPropertyOnNonEmptyEntityAndSyncFinalize() {
        //Setup mock entry for testing.
        OffsetDateTime lastWrite = OffsetDateTime.now();
        MockEntry mockEntry = newMockEntry( lastWrite );
        EntityDataValue edv = mockEntry.getValue();
        long version = edv.getVersion();
        OffsetDateTime preLastWrite = edv.getLastWrite();

        SetMultimap<UUID, Object> properties = HashMultimap.create();

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
        checkCorrectMetdata( mockProperties,
                propertyTypeId2,
                value2,
                lastWrite.toInstant().toEpochMilli(),
                Arrays.asList( lastWrite.toInstant().toEpochMilli() ),
                lastWrite );

        SyncFinalizer syncFinalizer = new SyncFinalizer( lastWrite );

        syncFinalizer.process( mockEntry );

        final long syncDeleteVersion = mockEntry.getValue().getProperties().get( propertyTypeId ).get( value )
                .getVersion();
        final long syncVersion = mockEntry.getValue().getProperties().get( propertyTypeId2 ).get( value2 ).getVersion();

        Assert.assertTrue(
                syncVersion >= lastWrite.toInstant().toEpochMilli() && syncVersion <= System.currentTimeMillis() );
        Assert.assertTrue( Math.abs( syncDeleteVersion ) >= lastWrite.toInstant().toEpochMilli()
                && Math.abs( syncDeleteVersion ) <= System.currentTimeMillis() );

        checkCorrectMetdata( mockProperties,
                propertyTypeId,
                value,
                syncDeleteVersion,
                Arrays.asList( version, syncDeleteVersion ),
                lastWrite );

        checkCorrectMetdata( mockProperties,
                propertyTypeId2,
                value2,
                lastWrite.toInstant().toEpochMilli(),
                Arrays.asList( lastWrite.toInstant().toEpochMilli() ),
                lastWrite );

        Assert.assertTrue( mockEntry.getValue().getVersion() >= lastWrite.toInstant().toEpochMilli() );
    }

    @Test
    public void addPropertiesAndMergeFinalize() {
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

        final long expectedVersion = lastWrite.toInstant().toEpochMilli();
        Map<UUID, Map<Object, PropertyMetadata>> mockProperties = mockEntry.getValue().getProperties();
        checkCorrectMetdata( mockProperties,
                propertyTypeId,
                value,
                expectedVersion,
                Arrays.asList( expectedVersion ),
                lastWrite );

        OffsetDateTime entityWrite = OffsetDateTime.now().plus( 1, ChronoUnit.SECONDS );
        MergeFinalizer mergeFinalizer = new MergeFinalizer( entityWrite );

        mergeFinalizer.process( mockEntry );

        Assert.assertEquals( mockEntry.getValue().getVersion(), entityWrite.toInstant().toEpochMilli() );
    }

    public static MockEntry newMockEntry( OffsetDateTime lastWrite ) {
        EntityDataKey edk = new EntityDataKey( UUID.randomUUID(), UUID.randomUUID() );

        OffsetDateTime preLastWrite = lastWrite.minus( 1, ChronoUnit.DAYS );

        EntityDataMetadata metadata = EntityDataMetadata.newEntityDataMetadata( preLastWrite );

        EntityDataValue edv = new EntityDataValue( metadata, new HashMap<>() );
        return new MockEntry( edk, edv );
    }

    public static UUID addProperty(
            Object value,
            Map<UUID, Map<Object, PropertyMetadata>> m,
            OffsetDateTime writeTime ) {
        UUID propertyTypeId = UUID.randomUUID();
        PropertyMetadata
                pm = PropertyMetadata.newPropertyMetadata( hashObject(value), writeTime );
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

    public static class MockEntry implements Entry<EntityDataKey, EntityDataValue> {
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
