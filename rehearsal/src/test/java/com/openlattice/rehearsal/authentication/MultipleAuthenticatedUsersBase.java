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

import com.google.common.collect.ImmutableSet;
import com.openlattice.authorization.AccessCheck;
import com.openlattice.authorization.AclKey;
import com.openlattice.authorization.AuthorizationsApi;
import com.openlattice.authorization.Permission;
import com.openlattice.authorization.PermissionsApi;
import com.openlattice.authorization.securable.SecurableObjectType;
import com.openlattice.data.DataApi;
import com.openlattice.edm.EdmApi;
import com.openlattice.edm.EntitySet;
import com.openlattice.edm.type.AssociationType;
import com.openlattice.edm.type.EntityType;
import com.openlattice.edm.type.PropertyType;
import com.openlattice.linking.LinkingApi;
import com.openlattice.linking.RealtimeLinkingApi;
import com.openlattice.mapstores.TestDataFactory;
import com.openlattice.organization.OrganizationsApi;
import com.openlattice.rehearsal.SetupEnvironment;
import com.openlattice.requests.RequestsApi;
import com.openlattice.search.SearchApi;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.junit.Assert;
import retrofit2.Retrofit;

public class MultipleAuthenticatedUsersBase extends SetupEnvironment {
    protected final static Map<String, Retrofit> retrofitMap = new HashMap<>();
    protected final static Map<String, Retrofit> indexerRetrofitMap = new HashMap<>();

    protected static EdmApi edmApi;
    protected static PermissionsApi permissionsApi;
    protected static AuthorizationsApi authorizationsApi;
    protected static RequestsApi requestsApi;
    protected static DataApi dataApi;
    protected static SearchApi searchApi;
    protected static OrganizationsApi organizationsApi;
    protected static LinkingApi linkingApi;
    protected static RealtimeLinkingApi realtimeLinkingApi;

    static {
        retrofitMap.put( "admin", retrofit );
        retrofitMap.put( "user1", retrofit1 );
        retrofitMap.put( "user2", retrofit2 );
        retrofitMap.put( "user3", retrofit3 );
        retrofitMap.put( "prod", retrofitProd );
        indexerRetrofitMap.put( "admin", retrofitIndexer );
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
        linkingApi = currentRetrofit.create( LinkingApi.class );

        Retrofit indexerRetrofit = indexerRetrofitMap.get( user );
        if ( indexerRetrofit != null ) {
            realtimeLinkingApi = indexerRetrofit.create( RealtimeLinkingApi.class );
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
        EnumSet.allOf( Permission.class ).forEach( permission -> {
            Assert.assertEquals( expectedPermissions.contains( permission ), permissionMap.get( permission ) );
        } );
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
        PropertyType pt = TestDataFactory.propertyType();
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

    private static EntityType createEntityType( Optional<FullQualifiedName> fqn,
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

    public static EntitySet createEntitySet( UUID entitySetId,
                                             EntityType entityType,
                                             boolean linking,
                                             Set<UUID> linkedEntitySetIds ) {
        EntitySet newES = new EntitySet(
                Optional.of( entitySetId ),
                entityType.getId(),
                RandomStringUtils.randomAlphanumeric( 10 ),
                "foobar",
                Optional.of( "barred" ),
                ImmutableSet.of( "foo@bar.com", "foobar@foo.net" ),
                Optional.of( linking ),
                Optional.of( linkedEntitySetIds ),
                Optional.of( true ) );

        Map<String, UUID> entitySetIds = edmApi.createEntitySets( ImmutableSet.of( newES ) );

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
}
