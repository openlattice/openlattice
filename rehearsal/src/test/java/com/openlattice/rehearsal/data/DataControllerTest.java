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

import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import com.google.common.collect.SetMultimap;
import com.openlattice.data.requests.EntitySetSelection;
import com.openlattice.data.requests.FileType;
import com.openlattice.edm.EntitySet;
import com.openlattice.edm.type.EntityType;
import com.openlattice.mapstores.TestDataFactory;
import com.openlattice.rehearsal.authentication.MultipleAuthenticatedUsersBase;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class DataControllerTest extends MultipleAuthenticatedUsersBase {

    private static final int    numberOfEntries = 10;
    private static final Random random          = new Random();

    @Test
    public void testCreateAndLoadEntityData() {
        EntityType et = createEntityType();
        EntitySet es = createEntitySet( et );
        UUID syncId = syncApi.getCurrentSyncId( es.getId() );

        dataApi.createEntityData( es.getId(),
                syncId,
                TestDataFactory.randomStringEntityData( numberOfEntries, et.getProperties() ) );
        EntitySetSelection ess = new EntitySetSelection( Optional.of( syncId ), Optional.of( et.getProperties() ) );
        Iterable<SetMultimap<FullQualifiedName, Object>> results = dataApi
                .loadEntitySetData( es.getId(), ess, FileType.json );
        Assert.assertEquals( numberOfEntries, Iterables.size( results ) );
    }

    @Test
    public void testLoadSelectedEntityData() {
        EntityType et = createEntityType();
        EntitySet es = createEntitySet( et );
        UUID syncId = syncApi.getCurrentSyncId( es.getId() );

        Map<String, SetMultimap<UUID, Object>> entities = TestDataFactory.randomStringEntityData( numberOfEntries,
                et.getProperties() );
        dataApi.createEntityData( es.getId(), syncId, entities );

        // load selected data
        Set<UUID> selectedProperties = et.getProperties().stream().filter( pid -> random.nextBoolean() )
                .collect( Collectors.toSet() );
        EntitySetSelection ess = new EntitySetSelection(
                Optional.of( syncId ),
                Optional.of( selectedProperties ) );
        Iterable<SetMultimap<FullQualifiedName, Object>> results = dataApi.loadEntitySetData( es.getId(), ess, null );

        // check results
        // For each entity, collect its property value in one set, and collect all these sets together.
        Set<Set<String>> resultValues = new HashSet<>();
        for ( SetMultimap<FullQualifiedName, Object> entity : results ) {
            resultValues.add( entity.asMap().values().stream().flatMap( e -> e.stream() ).map( o -> (String) o )
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

    @Test
    public void testSyncTicketService() {
        EntityType et = createEntityType();
        EntitySet es = createEntitySet( et );
        UUID syncId = syncApi.getCurrentSyncId( es.getId() );

        UUID ticket = dataApi.acquireSyncTicket( es.getId(), syncId );

        dataApi.storeEntityData( ticket,
                syncId,
                TestDataFactory.randomStringEntityData( numberOfEntries, et.getProperties() ) );

        dataApi.releaseSyncTicket( ticket );

        // not passing in token should retain current security context
        Iterable<SetMultimap<FullQualifiedName, Object>> results = dataApi.loadEntitySetData( es.getId(), null, "" );
        Assert.assertEquals( numberOfEntries, Iterables.size( results ) );

    }

    @BeforeClass
    public static void init() {
        loginAs( "admin" );
    }
}
