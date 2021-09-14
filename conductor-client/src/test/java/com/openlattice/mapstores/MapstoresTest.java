

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

package com.openlattice.mapstores;

import com.google.common.collect.ImmutableSet;
import com.kryptnostic.rhizome.mapstores.TestableSelfRegisteringMapStore;
import com.openlattice.authorization.AceKey;
import com.openlattice.authorization.AceValue;
import com.openlattice.authorization.HzAuthzTest;
import com.openlattice.authorization.Principal;
import com.openlattice.authorization.mapstores.PostgresCredentialMapstore;
import com.openlattice.authorization.mapstores.UserMapstore;
import com.openlattice.authorization.securable.AbstractSecurableObject;
import com.openlattice.datastore.services.EdmService;
import com.openlattice.datastore.services.EntitySetManager;
import com.openlattice.edm.EntitySet;
import com.openlattice.edm.type.EntityType;
import com.openlattice.edm.type.PropertyType;
import com.openlattice.hazelcast.HazelcastMap;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MapstoresTest extends HzAuthzTest {
    private static final Logger                                       logger   = LoggerFactory
            .getLogger( MapstoresTest.class );
    private static final Set<String>                                  excluded = ImmutableSet
            .of( HazelcastMap.PERMISSIONS.name() );
    @SuppressWarnings( "rawtypes" )
    private static final Map<String, TestableSelfRegisteringMapStore> mapstoreMap;
    private static final Collection<TestableSelfRegisteringMapStore>  mapstores;

    static {
        mapstores = testServer.getContext().getBeansOfType( TestableSelfRegisteringMapStore.class ).values();
        mapstoreMap = mapstores.stream().collect( Collectors.toMap( TestableSelfRegisteringMapStore::getMapName,
                Function.identity() ) );
    }

    @Test
    public void testPermissionMapstore() {
        TestableSelfRegisteringMapStore permissions = mapstoreMap.get( HazelcastMap.PERMISSIONS.name() );
        TestableSelfRegisteringMapStore objectTypes = mapstoreMap.get( HazelcastMap.SECURABLE_OBJECT_TYPES.name() );

        AceValue expected = (AceValue) permissions.generateTestValue();
        AceKey key = (AceKey) permissions.generateTestKey();

        Object actual;
        try {
            objectTypes.store( key.getAclKey(), expected.getSecurableObjectType() );
            permissions.store( key, expected );
            actual = permissions.load( key );
            if ( !expected.equals( actual ) ) {
                logger.error( "Incorrect r/w to mapstore {} for key {}. expected({}) != actual({})",
                        permissions.getMapName(),
                        key,
                        expected,
                        actual );
            }
            Assert.assertEquals( expected, actual );
        } catch ( UnsupportedOperationException e ) {
            logger.info( "Mapstore not implemented." );
        } catch ( Exception e ) {
            logger.error( "Unable to r/w to mapstore {} value: ({},{})", permissions.getMapName(), key, expected, e );
            throw e;
        }
    }

    @Test
    public void testMapstore() {
        mapstores.stream()
                .filter( ms -> !excluded.contains( ms.getMapName() ) )
                .forEach( MapstoresTest::test );
    }

    @Ignore
    @Test
    public void testDataMapstore() throws InterruptedException {
        EdmService edm = testServer.getContext().getBean( EdmService.class );
        var esm = testServer.getContext().getBean( EntitySetManager.class );

        PropertyType[] propertyTypes = new PropertyType[] {
                TestDataFactory.propertyType( EdmPrimitiveTypeKind.String ),
                TestDataFactory.propertyType( EdmPrimitiveTypeKind.Int64 ),
                TestDataFactory.propertyType( EdmPrimitiveTypeKind.DateTimeOffset ),
                TestDataFactory.propertyType( EdmPrimitiveTypeKind.Date ),
                TestDataFactory.propertyType( EdmPrimitiveTypeKind.TimeOfDay ),
                TestDataFactory.propertyType( EdmPrimitiveTypeKind.Boolean ),
                TestDataFactory.propertyType( EdmPrimitiveTypeKind.Binary ),
                TestDataFactory.propertyType( EdmPrimitiveTypeKind.Guid ),
                TestDataFactory.propertyType( EdmPrimitiveTypeKind.Int32 ),
                TestDataFactory.propertyType( EdmPrimitiveTypeKind.Double ) };

        Stream.of( propertyTypes ).forEach( edm::createPropertyTypeIfNotExists );

        EntityType entityType = TestDataFactory.entityTypesFromKeyAndTypes( propertyTypes[ 7 ], propertyTypes );

        edm.createEntityType( entityType );

        EntitySet entitySet = TestDataFactory.entitySetWithType( entityType.getId() );
        Principal p = TestDataFactory.userPrincipal();
        esm.createEntitySet( p, entitySet );
    }

    @SuppressWarnings( { "rawtypes", "unchecked" } )
    private static void test( TestableSelfRegisteringMapStore ms ) {
        if ( ms instanceof PostgresCredentialMapstore || ms instanceof UserMapstore ) {
            return;
        }
        Object expected = ms.generateTestValue();
        Object key = ms.generateTestKey();
        if ( AbstractSecurableObject.class.isAssignableFrom( expected.getClass() )
                && UUID.class.equals( key.getClass() ) ) {
            key = ( (AbstractSecurableObject) expected ).getId();
        }
        Object actual;
        try {
            ms.store( key, expected );
            actual = ms.load( key );
            if ( !expected.equals( actual ) ) {
                logger.error( "Incorrect r/w to mapstore {} for key {}. expected({}) != actual({})",
                        ms.getMapName(),
                        key,
                        expected,
                        actual );
            }
            Assert.assertEquals( expected, actual );
        } catch ( UnsupportedOperationException e ) {
            logger.info( "Mapstore not implemented." );
        } catch ( Exception e ) {
            logger.error( "Unable to r/w to mapstore {} value: ({},{})", ms.getMapName(), key, expected, e );
            throw e;
        }
    }
}
