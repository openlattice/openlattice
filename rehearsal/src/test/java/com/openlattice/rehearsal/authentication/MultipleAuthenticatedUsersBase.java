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

import static com.openlattice.rehearsal.data.DataManagerTest.randomDouble;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.SetMultimap;
import com.openlattice.authorization.AccessCheck;
import com.openlattice.authorization.AclKey;
import com.openlattice.authorization.AuthorizationsApi;
import com.openlattice.authorization.Permission;
import com.openlattice.authorization.PermissionsApi;
import com.openlattice.data.DataApi;
import com.openlattice.edm.EdmApi;
import com.openlattice.edm.EntitySet;
import com.openlattice.edm.type.AssociationType;
import com.openlattice.edm.type.EntityType;
import com.openlattice.edm.type.PropertyType;
import com.openlattice.mapstores.TestDataFactory;
import com.openlattice.organization.OrganizationsApi;
import com.openlattice.rehearsal.SetupEnvironment;
import com.openlattice.requests.RequestsApi;
import com.openlattice.search.SearchApi;
import com.openlattice.sync.SyncApi;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.olingo.commons.api.edm.geo.Geospatial.Dimension;
import org.apache.olingo.commons.api.edm.geo.Point;
import org.apache.olingo.commons.api.edm.geo.SRID;
import org.junit.Assert;
import retrofit2.Retrofit;

public class MultipleAuthenticatedUsersBase extends SetupEnvironment {
    private static final List<EdmPrimitiveTypeKind> edmTypesList;
    private static final int                        edmTypesSize;
    private static final Random                     random      = new Random();
    private static final SRID                       srid        = SRID.valueOf( "4326" );
    protected static     Map<String, Retrofit>      retrofitMap = new HashMap<>();
    protected static     EdmApi                     edmApi;
    protected static     PermissionsApi             permissionsApi;
    protected static     AuthorizationsApi          authorizationsApi;
    protected static     RequestsApi                requestsApi;
    protected static     DataApi                    dataApi;
    protected static     SearchApi                  searchApi;
    protected static     OrganizationsApi           organizationsApi;
    protected static     SyncApi                    syncApi;

    static {
        edmTypesList = Arrays.asList(
                EdmPrimitiveTypeKind.Binary,
                EdmPrimitiveTypeKind.Boolean,
                EdmPrimitiveTypeKind.Byte,
                EdmPrimitiveTypeKind.SByte,
                EdmPrimitiveTypeKind.Date,
                EdmPrimitiveTypeKind.DateTimeOffset,
                EdmPrimitiveTypeKind.TimeOfDay,
                EdmPrimitiveTypeKind.Duration,
                EdmPrimitiveTypeKind.Decimal,
                EdmPrimitiveTypeKind.Single,
                EdmPrimitiveTypeKind.Double,
                EdmPrimitiveTypeKind.Guid,
                EdmPrimitiveTypeKind.Int16,
                EdmPrimitiveTypeKind.Int32,
                EdmPrimitiveTypeKind.Int64,
                EdmPrimitiveTypeKind.String,
                EdmPrimitiveTypeKind.GeographyPoint );
        edmTypesSize = edmTypesList.size();
        retrofitMap.put( "admin", retrofit );
        retrofitMap.put( "user1", retrofit1 );
        retrofitMap.put( "user2", retrofit2 );
        retrofitMap.put( "user3", retrofit3 );
    }

    private static PropertyType getRandomPropertyType( UUID id ) {
        EdmPrimitiveTypeKind type = getRandomEdmType();
        //        EdmPrimitiveTypeKind type = EdmPrimitiveTypeKind.Date;
        return new PropertyType(
                id,
                getFqnFromUuid( id ),
                "Property " + id.toString(),
                Optional.empty(),
                ImmutableSet.of(),
                type );

    }

    private static EdmPrimitiveTypeKind getRandomEdmType() {
        return edmTypesList.get( random.nextInt( edmTypesSize ) );
    }

    /**
     * See
     * http://docs.oasis-open.org/odata/odata-json-format/v4.0/errata03/os/odata-json-format-v4.0-errata03-os-complete.html#_Toc453766642
     */
    @SuppressWarnings( "unchecked" )
    private static Object getRandomValue( EdmPrimitiveTypeKind type ) {
        Object rawObj;
        switch ( type ) {
            case Binary:
                byte[] b = new byte[ 10 ];
                random.nextBytes( b );
                rawObj = b;
                break;
            case Boolean:
                rawObj = random.nextBoolean();
                break;
            case Byte:
                rawObj = (byte) random.nextInt( 8 );
                break;
            case Date:
                rawObj = LocalDate.now();
                break;
            case DateTimeOffset:
                rawObj = OffsetDateTime.now();
                break;
            case Decimal:
                rawObj = new BigDecimal( Math.random() );
                break;
            case Double:
                rawObj = random.nextDouble();
                break;
            case Duration:
                rawObj = Duration.ofMillis( random.nextLong() );
                break;
            case Guid:
                rawObj = UUID.randomUUID();
                break;
            case Int16:
                rawObj = (short) random.nextInt( Short.MAX_VALUE + 1 );
                break;
            case Int32:
                rawObj = random.nextInt();
                break;
            case Int64:
                rawObj = random.nextLong();
                break;
            case String:
                rawObj = RandomStringUtils.randomAlphanumeric( 10 );
                break;
            case SByte:
                rawObj = random.nextInt( 256 ) - 128;
                break;
            case Single:
                rawObj = random.nextFloat();
                break;
            case TimeOfDay:
                rawObj = random.nextInt( 24 ) + ":" + random.nextInt( 60 ) + ":" + random.nextInt( 60 ) + "."
                        + random.nextInt( 1000 );
                break;
            case GeographyPoint:
                Point pt = new Point( Dimension.GEOGRAPHY, srid );
                pt.setY( randomDouble( 90 ) );
                pt.setX( randomDouble( 180 ) );
                rawObj = pt;
                break;
            default:
                rawObj = null;
        }
        return rawObj;
    }

    public static Map<UUID, SetMultimap<UUID, Object>> randomBinaryData( UUID keyType, UUID binaryType ) {
        return new ImmutableMap.Builder<UUID, SetMultimap<UUID, Object>>()
                .put( UUID.randomUUID(), randomElement( keyType, binaryType ) )
                .put( UUID.randomUUID(), randomElement( keyType, binaryType ) )
                .build();
    }

    public static SetMultimap<UUID, Object> randomElement( UUID keyType, UUID binaryType ) {

        SetMultimap<UUID, Object> element = HashMultimap.create();
        element.put( keyType, RandomStringUtils.random( 5 ) );
        element.put( binaryType, RandomUtils.nextBytes( 128 ) );
        element.put( binaryType, RandomUtils.nextBytes( 128 ) );
        element.put( binaryType, RandomUtils.nextBytes( 128 ) );
        return element;
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
        syncApi = currentRetrofit.create( SyncApi.class );
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

    private static FullQualifiedName getFqnFromUuid( UUID propertyId ) {
        return new FullQualifiedName( "test", propertyId.toString() );
    }

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

    public static EntityType createEntityType( UUID... propertyTypes ) {
        PropertyType k = createPropertyType();
        EntityType expected = TestDataFactory.entityType( k );
        expected.removePropertyTypes( expected.getProperties() );

        if ( propertyTypes == null || propertyTypes.length == 0 ) {
            PropertyType p1 = createPropertyType();
            PropertyType p2 = createPropertyType();
            PropertyType p3 = getRandomPropertyType( UUID.randomUUID() );
            PropertyType p4 = getBinaryPropertyType();
            Set<UUID> pts = new HashSet<>();
            pts.addAll( ImmutableSet.of( k.getId(), p1.getId(), p2.getId(), p3.getId(), p4.getId() ) );
            pts.addAll( Arrays.asList( propertyTypes ) );
            expected.addPropertyTypes( ImmutableSet.copyOf( pts ) );
        } else {
            expected.addPropertyTypes( ImmutableSet.copyOf( propertyTypes ) );
        }

        UUID entityTypeId = edmApi.createEntityType( expected );
        Assert.assertNotNull( "Entity type creation shouldn't return null UUID.", entityTypeId );

        return expected;
    }

    public static PropertyType getBinaryPropertyType() {
        PropertyType pt = TestDataFactory.binaryPropertyType();
        UUID propertyTypeId = edmApi.createPropertyType( pt );

        Assert.assertNotNull( "Property type creation returned null value.", propertyTypeId );

        return pt;
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

    public static EntitySet createEntitySet( EntityType entityType ) {
        EntitySet newES = new EntitySet(
                UUID.randomUUID(),
                entityType.getId(),
                RandomStringUtils.randomAlphanumeric( 10 ),
                "foobar",
                Optional.<String>of( "barred" ),
                ImmutableSet.of( "foo@bar.com", "foobar@foo.net" ) );

        Map<String, UUID> entitySetIds = edmApi.createEntitySets( ImmutableSet.of( newES ) );

        Assert.assertTrue( "Entity Set creation does not return correct UUID",
                entitySetIds.values().contains( newES.getId() ) );

        return newES;
    }
}
