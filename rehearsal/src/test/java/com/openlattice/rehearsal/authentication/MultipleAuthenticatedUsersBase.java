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

package com.openlattice.rehearsal.authentication;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Streams;
import com.openlattice.analysis.AnalysisApi;
import com.openlattice.authorization.AccessCheck;
import com.openlattice.authorization.AclKey;
import com.openlattice.authorization.AuthorizationsApi;
import com.openlattice.authorization.Permission;
import com.openlattice.authorization.PermissionsApi;
import com.openlattice.authorization.securable.SecurableObjectType;
import com.openlattice.client.RetrofitFactory;
import com.openlattice.data.DataApi;
import com.openlattice.data.DataEdge;
import com.openlattice.data.EntityDataKey;
import com.openlattice.directory.PrincipalApi;
import com.openlattice.edm.EdmApi;
import com.openlattice.edm.EntitySet;
import com.openlattice.edm.set.EntitySetFlag;
import com.openlattice.edm.type.AssociationType;
import com.openlattice.edm.type.EntityType;
import com.openlattice.edm.type.PropertyType;
import com.openlattice.entitysets.EntitySetsApi;
import com.openlattice.linking.LinkingFeedbackApi;
import com.openlattice.linking.RealtimeLinkingApi;
import com.openlattice.mapstores.TestDataFactory;
import com.openlattice.organization.OrganizationsApi;
import com.openlattice.postgres.IndexType;
import com.openlattice.rehearsal.GeneralException;
import com.openlattice.rehearsal.SetupEnvironment;
import com.openlattice.requests.RequestsApi;
import com.openlattice.search.SearchApi;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import kotlin.Pair;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.junit.Assert;
import retrofit2.Retrofit;

public class MultipleAuthenticatedUsersBase extends SetupEnvironment {
    private final static Map<String, Retrofit>     retrofitMap       = new HashMap<>();
    private final static Map<String, Retrofit>     linkerRetrofitMap = new HashMap<>();
    private final static Map<String, OkHttpClient> httpClientMap     = new HashMap<>();

    protected static EdmApi             edmApi;
    protected static PermissionsApi     permissionsApi;
    protected static AuthorizationsApi  authorizationsApi;
    protected static RequestsApi        requestsApi;
    protected static DataApi            dataApi;
    protected static SearchApi          searchApi;
    protected static OrganizationsApi   organizationsApi;
    protected static EntitySetsApi      entitySetsApi;
    protected static RealtimeLinkingApi realtimeLinkingApi;
    protected static AnalysisApi        analysisApi;
    protected static LinkingFeedbackApi linkingFeedbackApi;
    protected static PrincipalApi       principalApi;

    protected static OkHttpClient currentHttpClient;

    static {
        retrofitMap.put( "admin", retrofit );
        retrofitMap.put( "user1", retrofit1 );
        retrofitMap.put( "user2", retrofit2 );
        retrofitMap.put( "user3", retrofit3 );
        retrofitMap.put( "prod", retrofitProd );
        linkerRetrofitMap.put( "admin", retrofitLinker );

        httpClientMap.put( "admin", httpClient );
        httpClientMap.put( "user1", httpClient1 );
        httpClientMap.put( "user2", httpClient2 );

    }

    /**
     * Auxiliary functions
     */

    public static void loginAs( String user ) {
        // update Api instances involved
        Retrofit currentRetrofit = retrofitMap.get( user );
        if ( currentRetrofit == null ) {
            throw new IllegalArgumentException( "User does not exists in Retrofit map." );
        }
        edmApi = currentRetrofit.create( EdmApi.class );
        permissionsApi = currentRetrofit.create( PermissionsApi.class );
        authorizationsApi = currentRetrofit.create( AuthorizationsApi.class );
        requestsApi = currentRetrofit.create( RequestsApi.class );
        dataApi = currentRetrofit.create( DataApi.class );
        searchApi = currentRetrofit.create( SearchApi.class );
        organizationsApi = currentRetrofit.create( OrganizationsApi.class );
        entitySetsApi = currentRetrofit.create( EntitySetsApi.class );
        analysisApi = currentRetrofit.create( AnalysisApi.class );
        principalApi = currentRetrofit.create( PrincipalApi.class );

        Retrofit linkerRetrofit = linkerRetrofitMap.get( user );
        if ( linkerRetrofit != null ) {
            realtimeLinkingApi = linkerRetrofit.create( RealtimeLinkingApi.class );
            linkingFeedbackApi = linkerRetrofit.create( LinkingFeedbackApi.class );
        }

        currentHttpClient = httpClientMap.get( user );
    }

    /**
     * Helper functions to make direct HTTP calls
     */

    public static Response makePutRequest( String url, RequestBody body ) throws GeneralException {
        Request request = new Request.Builder()
                .url( RetrofitFactory.Environment.TESTING.getBaseUrl() + url.substring( 1 ) ) // remove extra "/"
                .put( body )
                .build();
        try {
            Response response = currentHttpClient.newCall( request ).execute();
            if ( !response.isSuccessful() ) {
                String errorBody = IOUtils.toString( response.body().byteStream(), Charsets.UTF_8 );
                throw new GeneralException( errorBody );
            }

            return response;
        } catch ( IOException ex ) {
            throw new GeneralException( "Something went wrong with call: " + request );
        }
    }

    public static Response makeDeleteRequest( String url ) throws GeneralException {
        Request request = new Request.Builder()
                .url( RetrofitFactory.Environment.TESTING.getBaseUrl() + url.substring( 1 ) ) // remove extra "/"
                .delete()
                .build();
        try {
            Response response = currentHttpClient.newCall( request ).execute();
            if ( !response.isSuccessful() ) {
                String errorBody = IOUtils.toString( response.body().byteStream(), Charsets.UTF_8 );
                throw new GeneralException( errorBody );
            }

            return response;
        } catch ( IOException ex ) {
            throw new GeneralException( "Something went wrong with call: " + request );
        }
    }

    public static PropertyType getBinaryPropertyType() {
        PropertyType pt = TestDataFactory.binaryPropertyType();
        UUID propertyTypeId = edmApi.createPropertyType( pt );
        Assert.assertNotNull( "Property type creation returned null value.", propertyTypeId );
        return pt;
    }

    /**
     * Helper methods for AuthorizationsApi
     */

    public static void checkPermissionsMap(
            Map<Permission, Boolean> permissionMap,
            EnumSet<Permission> expectedPermissions ) {
        EnumSet.allOf( Permission.class ).forEach( permission ->
                Assert.assertEquals( expectedPermissions.contains( permission ), permissionMap.get( permission ) )
        );
    }

    public static void checkUserPermissions( AclKey aclKey, EnumSet<Permission> expected ) {

        authorizationsApi
                .checkAuthorizations( ImmutableSet.of( new AccessCheck( aclKey, EnumSet.allOf( Permission.class ) ) ) )
                .forEach( auth -> checkPermissionsMap( auth.getPermissions(), expected ) );
    }

    /**
     * Helper methods for EdmApi
     */

    public static PropertyType createDatePropertyType() {
        PropertyType pt = TestDataFactory.datePropertyType();
        UUID propertyTypeId = edmApi.createPropertyType( pt );

        Assert.assertNotNull( "Property type creation returned null value.", propertyTypeId );

        return pt;
    }

    public static PropertyType createDateTimePropertyType() {
        PropertyType pt = TestDataFactory.dateTimePropertyType();
        UUID propertyTypeId = edmApi.createPropertyType( pt );

        Assert.assertNotNull( "Property type creation returned null value.", propertyTypeId );

        return pt;
    }

    public static PropertyType createPropertyType() {
        PropertyType pt = TestDataFactory.propertyType( IndexType.BTREE );
        UUID propertyTypeId = edmApi.createPropertyType( pt );

        Assert.assertNotNull( "Property type creation returned null value.", propertyTypeId );

        return pt;
    }

    public static EntityType createEntityType( FullQualifiedName fqn ) {
        return createEntityType( Optional.of( fqn ), SecurableObjectType.EntityType );
    }

    public static EntityType createEntityType( UUID... propertyTypes ) {
        return createEntityType( Optional.empty(), SecurableObjectType.EntityType, propertyTypes );
    }

    public static EntityType createEdgeEntityType( UUID... propertyTypes ) {
        return createEntityType( Optional.empty(), SecurableObjectType.AssociationType, propertyTypes );
    }

    private static EntityType createEntityType(
            Optional<FullQualifiedName> fqn,
            SecurableObjectType category,
            UUID... propertyTypes ) {
        PropertyType k = createPropertyType();
        EntityType expected = TestDataFactory.entityType( fqn, category, k );
        expected.removePropertyTypes( expected.getProperties() );

        if ( propertyTypes == null || propertyTypes.length == 0 ) {
            PropertyType p1 = createPropertyType();
            PropertyType p2 = createPropertyType();
            expected.addPropertyTypes( ImmutableSet.of( k.getId(), p1.getId(), p2.getId() ) );
        } else {
            expected.addPropertyTypes( ImmutableSet.copyOf( propertyTypes ) );
            expected.addPropertyTypes( ImmutableSet.of( k.getId() ) );
        }

        UUID entityTypeId = edmApi.createEntityType( expected );
        Assert.assertNotNull( "Entity type creation shouldn't return null UUID.", entityTypeId );

        return expected;
    }

    public static AssociationType createAssociationType(
            EntityType aet,
            Set<EntityType> src,
            Set<EntityType> dst ) {

        AssociationType expected = new AssociationType( Optional.of( aet ),
                src.stream().map( EntityType::getId ).collect( Collectors.toCollection( LinkedHashSet::new ) ),
                dst.stream().map( EntityType::getId ).collect( Collectors.toCollection( LinkedHashSet::new ) ),
                false );

        UUID associationTypeId = edmApi.createAssociationType( expected );
        Assert.assertNotNull( "Assert association type shouldn't return null UUID.", associationTypeId );

        return expected;
    }

    public static EntitySet createEntitySet() {
        EntityType entityType = createEntityType();
        return createEntitySet( entityType );
    }

    public static EntitySet createEntitySet( EntityType entityType, boolean linking, Set<UUID> linkedEntitySetIds ) {
        return createEntitySet( UUID.randomUUID(), entityType, linking, linkedEntitySetIds );
    }

    public static EntitySet createEntitySet(
            UUID entitySetId,
            EntityType entityType,
            boolean linking,
            Set<UUID> linkedEntitySetIds ) {
        EnumSet<EntitySetFlag> flags = EnumSet.of( EntitySetFlag.EXTERNAL );
        if ( linking ) {
            flags.add( EntitySetFlag.LINKING );
        }
        EntitySet newES = new EntitySet(
                Optional.of( entitySetId ),
                entityType.getId(),
                RandomStringUtils.randomAlphanumeric( 10 ),
                "foobar",
                Optional.of( "barred" ),
                ImmutableSet.of( "foo@bar.com", "foobar@foo.net" ),
                Optional.of( linkedEntitySetIds ),
                Optional.empty(),
                Optional.of(flags),
                Optional.empty());

        Map<String, UUID> entitySetIds = entitySetsApi.createEntitySets( ImmutableSet.of( newES ) );

        Assert.assertTrue( "Entity Set creation does not return correct UUID",
                entitySetIds.values().contains( newES.getId() ) );

        return newES;
    }

    public static EntitySet createEntitySet( EntityType entityType ) {
        return createEntitySet( entityType, false, new HashSet<>() );
    }

    public static EntitySet createEntitySet( UUID entitySetId, EntityType entityType ) {
        return createEntitySet( entitySetId, entityType, false, new HashSet<>() );
    }

    public static Pair<UUID, List<DataEdge>> createDataEdges(
            UUID edgeEntitySetId,
            UUID srcEntitySetId,
            UUID dstEntitySetId,
            Set<UUID> properties,
            List<UUID> srcIds,
            List<UUID> dstIds,
            int numberOfEntries ) {
        List<Map<UUID, Set<Object>>> edgeData = Lists.newArrayList(
                TestDataFactory.randomStringEntityData( numberOfEntries, properties ).values() );

        List<DataEdge> edges = Streams
                .mapWithIndex(
                        Stream.of( srcIds.toArray() ),
                        ( data, index ) -> {
                            int idx = (int) index;
                            EntityDataKey srcDataKey = new EntityDataKey( srcEntitySetId, srcIds.get( idx ) );
                            EntityDataKey dstDataKey = new EntityDataKey( dstEntitySetId, dstIds.get( idx ) );
                            return new DataEdge( srcDataKey, dstDataKey, edgeData.get( idx ) );
                        } )
                .collect( Collectors.toList() );

        return new Pair<>( edgeEntitySetId, edges );
    }
}
