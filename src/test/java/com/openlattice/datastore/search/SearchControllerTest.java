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

package com.openlattice.datastore.search;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.openlattice.conductor.rpc.ConductorElasticsearchApi;
import com.openlattice.datastore.authentication.MultipleAuthenticatedUsersBase;
import com.openlattice.edm.EntitySet;
import com.openlattice.edm.type.EntityType;
import com.openlattice.search.requests.Search;
import com.openlattice.search.requests.SearchResult;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class SearchControllerTest extends MultipleAuthenticatedUsersBase {

    // All random entity sets generated has title foobar and description barred
    private static final String title       = "foobar";
    private static final String description = "barred";

    private static EntityType et1;
    private static EntityType et2;
    private static EntitySet  es1;
    private static EntitySet  es2;

    @SuppressWarnings( "unchecked" )
    @Test
    public void testSearchByEntityType() {
        Search req = new Search( Optional.empty(), Optional.of( et1.getId() ), Optional.empty(), 0, 10 );
        SearchResult results = searchApi.executeEntitySetKeywordQuery( req );

        Assert.assertEquals( 1, results.getNumHits() );
        Map<String, Object> result = (Map) results.getHits().iterator().next()
                .get( ConductorElasticsearchApi.ENTITY_SET );
        Assert.assertEquals( es1.getName(), (String) result.get( "name" ) );
    }

    @Test
    public void testSearchByPropertyType() {
        Search req = new Search(
                Optional.empty(),
                Optional.empty(),
                Optional.of( et1.getProperties() ),
                0,
                10 );
        SearchResult results = searchApi.executeEntitySetKeywordQuery( req );

        Assert.assertEquals( 1, results.getNumHits() );
        Map<String, Object> result = (Map) results.getHits().iterator().next()
                .get( ConductorElasticsearchApi.ENTITY_SET );
        Assert.assertEquals( es1.getName(), (String) result.get( ConductorElasticsearchApi.NAME ) );
    }

    @Test
    public void testSearchByTitleKeyword() {
        Search req = new Search( Optional.of( title ), Optional.empty(), Optional.empty(), 0, 10 );
        SearchResult results = searchApi.executeEntitySetKeywordQuery( req );

        // Filter search results to those that matches names of the two entity sets we created
        Set<String> names = ImmutableSet.of( es1.getName(), es2.getName() );
        Iterable<Map<String, Object>> filtered = Iterables
                .filter( results.getHits(), result -> matchName( result, names ) );

        Assert.assertEquals( 2, Iterables.size( filtered ) );
    }

    @Test
    public void testSearchByDescriptionKeyword() {
        Search req = new Search( Optional.of( description ), Optional.empty(), Optional.empty(), 0, 10 );
        SearchResult results = searchApi.executeEntitySetKeywordQuery( req );

        // Filter search results to those that matches names of the two entity sets we created
        Set<String> names = ImmutableSet.of( es1.getName(), es2.getName() );
        Iterable<Map<String, Object>> filtered = Iterables
                .filter( results.getHits(), result -> matchName( result, names ) );

        Assert.assertEquals( 2, Iterables.size( filtered ) );
    }

    @SuppressWarnings( "unchecked" )
    private boolean matchName( Map<String, Object> result, Set<String> names ) {
        Map<String, Object> entitySet = (Map<String, Object>) result.get( ConductorElasticsearchApi.ENTITY_SET );
        return names.contains( (String) entitySet.get( ConductorElasticsearchApi.NAME ) );
    }

    @BeforeClass
    public static void init() {
        loginAs( "admin" );
        et1 = createEntityType();
        et2 = createEntityType();
        es1 = createEntitySet( et1 );
        es2 = createEntitySet( et2 );

        System.out.println( "Entity Types created: " );
        System.out.println( et1 );
        System.out.println( et2 );

        System.out.println( "Entity Sets created: " );
        System.out.println( es1 );
        System.out.println( es2 );
        try {
            Thread.sleep( 5000 );
        } catch ( InterruptedException e ) {
            System.err.println( "The wait for elastic search indexing is disrupted." );
        }

    }
}
