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

package com.openlattice.rehearsal.data;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.openlattice.data.requests.EntitySetSelection;
import com.openlattice.data.requests.FileType;
import com.openlattice.edm.EntitySet;
import com.openlattice.edm.type.EntityType;
import com.openlattice.edm.type.PropertyType;
import com.openlattice.mapstores.TestDataFactory;
import com.openlattice.postgres.DataTables;
import com.openlattice.rehearsal.authentication.MultipleAuthenticatedUsersBase;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class DataControllerTest2 extends MultipleAuthenticatedUsersBase {

    private static final int    numberOfEntries = 10;
    private static final Random random          = new Random();

    @Test
    public void testCreateAndLoadEntityData() {
        EntityType et = createEntityType();
        waitForIt();
        EntitySet es = createEntitySet( et );
        waitForIt();

        Map<UUID, SetMultimap<UUID, Object>> testData = TestDataFactory
                .randomStringEntityData( numberOfEntries, et.getProperties() );
        dataApi.replaceEntities( es.getId(), testData, false );
        EntitySetSelection ess = new EntitySetSelection( Optional.of( et.getProperties() ) );
        Set<SetMultimap<FullQualifiedName, Object>> results = Sets.newHashSet( dataApi
                .loadEntitySetData( es.getId(), ess, FileType.json ) );

        Assert.assertEquals( numberOfEntries, results.size() );
    }

    @Test
    public void testCreateLoadReplaceLoadData() {
        EntityType et = createEntityType();
        waitForIt();
        EntitySet es = createEntitySet( et );
        waitForIt();

        Map<UUID, SetMultimap<UUID, Object>> testData = TestDataFactory
                .randomStringEntityData( numberOfEntries, et.getProperties() );
        final List<SetMultimap<UUID, Object>> entries = ImmutableList.copyOf( testData.values() );
        final List<UUID> ids = dataApi.createOrMergeEntities( es.getId(), entries );

        final EntitySetSelection ess = new EntitySetSelection(
                Optional.of( et.getProperties() ),
                Optional.of( new HashSet<>( ids ) ) );
        final List<SetMultimap<FullQualifiedName, Object>> data = ImmutableList
                .copyOf( dataApi.loadEntitySetData( es.getId(), ess, FileType.json ) );
        final Map<UUID, SetMultimap<FullQualifiedName, Object>> indexActual = index( data );
    }

    private Map<UUID, SetMultimap<FullQualifiedName, Object>> index( Collection<SetMultimap<FullQualifiedName, Object>> data ) {
        return data.stream().collect(
                Collectors.toMap(
                        e -> UUID.fromString( (String) e.get( DataTables.ID_FQN ).iterator().next() ),
                        Function.identity() ) );
    }

    private void waitForIt() {
        try {
            Thread.sleep( 1000 );
        } catch ( InterruptedException e ) {
            throw new IllegalStateException( "Failed to wait for it." );
        }
    }

    @Test
    public void testDateTypes() {
        PropertyType p1 = createDateTimePropertyType();
        PropertyType k = createPropertyType();
        PropertyType p2 = createDatePropertyType();

        EntityType et = TestDataFactory.entityType( k );

        et.removePropertyTypes( et.getProperties() );
        et.addPropertyTypes( ImmutableSet.of( k.getId(), p1.getId(), p2.getId() ) );

        UUID entityTypeId = edmApi.createEntityType( et );
        Assert.assertNotNull( "Entity type creation shouldn't return null UUID.", entityTypeId );

        waitForIt();
        EntitySet es = createEntitySet( et );
        waitForIt();

        Map<UUID, SetMultimap<UUID, Object>> testData = new HashMap<>();
        LocalDate d = LocalDate.now();
        OffsetDateTime odt = OffsetDateTime.ofInstant( Instant.now(), ZoneId.of( "UTC" ) );
        testData.put( UUID.randomUUID(),
                ImmutableSetMultimap
                        .of( p1.getId(), odt, p2.getId(), d, k.getId(), RandomStringUtils.randomAlphanumeric( 5 ) ) );
        dataApi.replaceEntities( es.getId(), testData, false );
        EntitySetSelection ess = new EntitySetSelection( Optional.of( et.getProperties() ) );
        Set<SetMultimap<FullQualifiedName, Object>> results = Sets.newHashSet( dataApi
                .loadEntitySetData( es.getId(), ess, FileType.json ) );

        Assert.assertEquals( testData.size(), results.size() );
        SetMultimap<FullQualifiedName, Object> result = results.iterator().next();
        OffsetDateTime p1v = OffsetDateTime.parse( (CharSequence) result.get( p1.getType() ).iterator().next() );
        LocalDate p2v = LocalDate.parse( (CharSequence) result.get( p2.getType() ).iterator().next() );

        Assert.assertEquals( odt, p1v );
        Assert.assertEquals( d, p2v );
    }

    @Test
    public void testLoadSelectedEntityData() {
        EntityType et = createEntityType();
        waitForIt();
        EntitySet es = createEntitySet( et );
        waitForIt();

        Map<UUID, SetMultimap<UUID, Object>> entities = TestDataFactory.randomStringEntityData( numberOfEntries,
                et.getProperties() );
        dataApi.replaceEntities( es.getId(), entities, false );

        // load selected data
        Set<UUID> selectedProperties = et.getProperties().stream().filter( pid -> random.nextBoolean() )
                .collect( Collectors.toSet() );
        EntitySetSelection ess = new EntitySetSelection( Optional.of( selectedProperties ) );
        Iterable<SetMultimap<FullQualifiedName, Object>> results = dataApi.loadEntitySetData( es.getId(), ess, null );

        // check results
        // For each entity, collect its property value in one set, and collect all these sets together.
        Set<Set<String>> resultValues = new HashSet<>();
        for ( SetMultimap<FullQualifiedName, Object> entity : results ) {
            resultValues.add( entity.asMap().entrySet().stream()
                    .filter( e -> !e.getKey().getFullQualifiedNameAsString().contains( "@" ) )
                    .flatMap( e -> e.getValue().stream() )
                    .map( o -> (String) o )
                    .collect( Collectors.toSet() ) );
        }

        Set<Set<String>> expectedValues = new HashSet<>();
        for ( SetMultimap<UUID, Object> entity : entities.values() ) {
            expectedValues
                    .add( entity.asMap().entrySet().stream()
                            // filter the entries with key (propertyId) in the selected set
                            .filter( e -> ( selectedProperties.isEmpty() || selectedProperties
                                    .contains( e.getKey() ) ) )
                            // Put all the property values in the same stream, and cast them back to strings
                            .flatMap( e -> e.getValue().stream() )
                            .map( o -> (String) o )
                            .collect( Collectors.toSet() ) );
        }

        Assert.assertEquals( expectedValues, resultValues );
    }

    @BeforeClass
    public static void init() {
        loginAs( "admin" );
    }
}
