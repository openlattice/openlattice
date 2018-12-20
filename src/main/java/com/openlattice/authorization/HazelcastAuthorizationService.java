

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

package com.openlattice.authorization;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Maps.transformValues;
import static com.openlattice.authorization.mapstores.PermissionMapstore.ACL_KEY_INDEX;

import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.MapMaker;
import com.google.common.eventbus.EventBus;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.openlattice.authorization.mapstores.PermissionMapstore;
import com.openlattice.authorization.paging.AuthorizedObjectsSearchResult;
import com.openlattice.authorization.processors.PermissionMerger;
import com.openlattice.authorization.processors.PermissionRemover;
import com.openlattice.authorization.processors.SecurableObjectTypeUpdater;
import com.openlattice.authorization.securable.SecurableObjectType;
import com.openlattice.hazelcast.HazelcastMap;

import java.time.OffsetDateTime;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HazelcastAuthorizationService implements AuthorizationManager {
    private static final Logger logger = LoggerFactory.getLogger( AuthorizationManager.class );

    private final IMap<AclKey, SecurableObjectType> securableObjectTypes;
    private final IMap<AceKey, AceValue>            aces;
    private final AuthorizationQueryService         aqs;
    private final EventBus                          eventBus;

    public HazelcastAuthorizationService(
            HazelcastInstance hazelcastInstance,
            AuthorizationQueryService aqs,
            EventBus eventBus ) {
        this.aces = hazelcastInstance.getMap( HazelcastMap.PERMISSIONS.name() );
        this.securableObjectTypes = hazelcastInstance.getMap( HazelcastMap.SECURABLE_OBJECT_TYPES.name() );
        this.aqs = checkNotNull( aqs );
        this.eventBus = checkNotNull( eventBus );
    }

    @Override
    public void setSecurableObjectType( AclKey aclKey, SecurableObjectType objectType ) {
        securableObjectTypes.set( aclKey, objectType );
        aces.executeOnEntries( new SecurableObjectTypeUpdater( objectType ), hasAclKey( aclKey ) );
    }

    @Override
    public void addPermission(
            AclKey key,
            Principal principal,
            EnumSet<Permission> permissions ) {
        //TODO: We should do something better than reading the securable object type.
        OffsetDateTime expirationDate = OffsetDateTime.MAX;
        SecurableObjectType securableObjectType = securableObjectTypes.getOrDefault( key, SecurableObjectType.Unknown );
        if ( securableObjectType == SecurableObjectType.Unknown ) {
            logger.warn( "Unrecognized object type for acl key {} key ", key );
        }
        aces.executeOnKey( new AceKey( key, principal ),
                new PermissionMerger( permissions, securableObjectType, expirationDate ) );
    }

    @Override
    public void addPermission(
            AclKey key,
            Principal principal,
            EnumSet<Permission> permissions,
            OffsetDateTime expirationDate ) {
        //TODO: We should do something better than reading the securable object type.
        SecurableObjectType securableObjectType = securableObjectTypes.getOrDefault( key, SecurableObjectType.Unknown );
        if ( securableObjectType == SecurableObjectType.Unknown ) {
            logger.warn( "Unrecognized object type for acl key {} key ", key );
        }
        aces.executeOnKey( new AceKey( key, principal ),
                new PermissionMerger( permissions, securableObjectType, expirationDate ) );
    }

    @Override
    public void removePermission(
            AclKey key,
            Principal principal,
            EnumSet<Permission> permissions ) {
        aces.executeOnKey( new AceKey( key, principal ), new PermissionRemover( permissions ) );
    }

    @Override
    public void setPermission(
            AclKey key,
            Principal principal,
            EnumSet<Permission> permissions ) {
        //This should be a rare call to overwrite all permissions, so it's okay to do a read before write.
        OffsetDateTime expirationDate = OffsetDateTime.MAX;
        SecurableObjectType securableObjectType = securableObjectTypes.getOrDefault( key, SecurableObjectType.Unknown );
        aces.set( new AceKey( key, principal ), new AceValue( permissions, securableObjectType, expirationDate ) );
    }

    @Override
    public void setPermission(
            AclKey key,
            Principal principal,
            EnumSet<Permission> permissions,
            OffsetDateTime expirationDate ) {
        //This should be a rare call to overwrite all permissions, so it's okay to do a read before write.
        SecurableObjectType securableObjectType = securableObjectTypes.getOrDefault( key, SecurableObjectType.Unknown );
        aces.set( new AceKey( key, principal ), new AceValue( permissions, securableObjectType, expirationDate ) );
    }

    @Override
    public void deletePermissions( AclKey aclKey ) {
        securableObjectTypes.delete( aclKey );
        aces.removeAll( hasAclKey( aclKey ) );
        aqs.deletePermissionsByAclKeys( aclKey );
    }

    @Timed
    @Override
    public Map<AclKey, EnumMap<Permission, Boolean>> maybeFastAccessChecksForPrincipals(
            Set<AccessCheck> accessChecks,
            Set<Principal> principals ) {
        final Map<AclKey, EnumMap<Permission, Boolean>> results = new HashMap<>( accessChecks.size() );
        final Set<AceKey> aceKeys = new HashSet<>( accessChecks.size() * principals.size() );
        final Map<AceKey, AceValue> aceMap;
        Stopwatch w = Stopwatch.createStarted();
        //Prepare the results data structure

        accessChecks
                .parallelStream()
                .forEach( accessCheck -> {
                    AclKey aclKey = accessCheck.getAclKey();
                    EnumMap<Permission, Boolean> granted = results.get( aclKey );

                    if ( granted == null ) {
                        granted = new EnumMap<>( Permission.class );
                        results.put( aclKey, granted );
                    }

                    for ( Permission permission : accessCheck.getPermissions() ) {
                        granted.putIfAbsent( permission, false );
                    }

                    principals.forEach( p -> aceKeys.add( new AceKey( aclKey, p ) ) );
                } );
        logger.info( "Preparing result data structure took: {} ms", w.elapsed( TimeUnit.MILLISECONDS ) );

        w.reset();
        w.start();
        aceMap = aces.getAll( aceKeys );
        logger.info( "Preparing getting all data took: {} ms", w.elapsed( TimeUnit.MILLISECONDS ) );
        w.reset();
        w.start();

        aceMap.forEach( ( ak, av ) -> {
            EnumMap<Permission, Boolean> granted = results.get( ak.getAclKey() );
            av.getPermissions().forEach( p -> {
                if ( granted.containsKey( p ) ) {
                    granted.put( p, true );
                }
            } );
        } );

        logger.info( "Populating return map took: {} ms", w.elapsed( TimeUnit.MILLISECONDS ) );

        return results;
    }

    @Timed
    @Override
    public Map<AclKey, EnumMap<Permission, Boolean>> authorize(
            Map<AclKey, EnumSet<Permission>> requests,
            Set<Principal> principals ) {
        AuthorizationAggregator agg = aces
                .aggregate( new AuthorizationAggregator(
                                transformValues( requests, HazelcastAuthorizationService::noAccess ) ),
                        matches( requests.keySet(), principals ) );

        return agg.getPermissions();
    }

    @Timed
    @Override
    public Stream<Authorization> accessChecksForPrincipals(
            Set<AccessCheck> accessChecks,
            Set<Principal> principals ) {
        final Map<AclKey, EnumMap<Permission, Boolean>> results = new MapMaker()
                .concurrencyLevel( Runtime.getRuntime().availableProcessors() )
                .initialCapacity( accessChecks.size() )
                .makeMap();
        accessChecks
                .parallelStream()
                .forEach( accessCheck -> {
                    AclKey aclKey = accessCheck.getAclKey();
                    EnumMap<Permission, Boolean> granted = results.get( aclKey );

                    if ( granted == null ) {
                        granted = new EnumMap<>( Permission.class );
                        results.put( aclKey, granted );
                    }

                    for ( Permission permission : accessCheck.getPermissions() ) {
                        granted.putIfAbsent( permission, false );
                    }
                } );

        AuthorizationAggregator agg =
                aces.aggregate( new AuthorizationAggregator( results ), matches( results.keySet(), principals ) );

        return agg
                .getPermissions()
                .entrySet()
                .stream()
                .map( e -> new Authorization( e.getKey(), e.getValue() ) );

    }

    @Override
    public void deletePrincipalPermissions( Principal principal ) {
        aqs.deletePermissionsByPrincipal( principal );
    }

    public Predicate matches( AclKey aclKey, Collection<Principal> principals, EnumSet<Permission> permissions ) {
        return Predicates.and(
                hasAclKey( aclKey ),
                hasAnyPrincipals( principals ),
                hasAnyPermissions( permissions ) );
    }

    @Timed
    @Override
    public boolean checkIfHasPermissions(
            AclKey key,
            Set<Principal> principals,
            EnumSet<Permission> requiredPermissions ) {
        final var permissionsMap = new EnumMap<Permission, Boolean>( Permission.class );
        final var authzMap = ImmutableMap.of( key, permissionsMap );

        for ( Permission permission : requiredPermissions ) {
            permissionsMap.put( permission, false );
        }

        final EnumMap<Permission, Boolean> result =
                aces.aggregate( new AuthorizationAggregator( authzMap ), matches( authzMap.keySet(), principals ) )
                        .getPermissions()
                        .get( key );
        if ( result == null ) {
            return false;
        } else {
            return result.values().stream().allMatch( p -> p );
        }
    }

    @Override
    public boolean checkIfUserIsOwner( AclKey aclKey, Principal principal ) {
        checkArgument( principal.getType().equals( PrincipalType.USER ), "A role cannot be the owner of an object" );
        return checkIfHasPermissions( aclKey, ImmutableSet.of( principal ), EnumSet.of( Permission.OWNER ) );
    }

    @Override
    @Timed
    public Set<Permission> getSecurableObjectPermissions(
            AclKey key,
            Set<Principal> principals ) {
        final EnumSet<Permission> objectPermissions = EnumSet.noneOf( Permission.class );
        final Set<AceKey> aceKeys = principals
                .stream()
                .map( principal -> new AceKey( key, principal ) )
                .collect( Collectors.toSet() );
        aces
                .getAll( aceKeys )
                .values()
                .stream()
                //                .peek( ps -> logger.info( "Implementing class: {}", ps.getClass().getCanonicalName() ) )
                .map( AceValue::getPermissions )
                .filter( permissions -> permissions != null )
                .forEach( objectPermissions::addAll );
        return objectPermissions;
    }

    @Timed
    @Override
    public Stream<AclKey> getAuthorizedObjectsOfType(
            Principal principal,
            SecurableObjectType objectType,
            EnumSet<Permission> permissions ) {
        Predicate p = Predicates
                .and( hasPrincipal( principal ), hasType( objectType ), hasExactPermissions( permissions ) );
        return this.aces.keySet( p )
                .stream()
                .map( AceKey::getAclKey )
                .distinct();
    }

    @Timed
    @Override
    public Stream<AclKey> getAuthorizedObjectsOfType(
            Set<Principal> principals,
            SecurableObjectType objectType,
            EnumSet<Permission> permissions ) {
        Predicate p = Predicates
                .and( hasAnyPrincipals( principals ), hasType( objectType ), hasExactPermissions( permissions ) );
        return this.aces.keySet( p )
                .stream()
                .map( AceKey::getAclKey )
                .distinct();
    }

    @Timed
    @Override
    public AuthorizedObjectsSearchResult getAuthorizedObjectsOfType(
            NavigableSet<Principal> principals,
            SecurableObjectType objectType,
            Permission permission,
            String offset,
            int pageSize ) {

        return aqs.getAuthorizedAclKeys( principals, objectType, permission, offset, pageSize );
    }

    @Timed
    @Override
    public Acl getAllSecurableObjectPermissions( AclKey key ) {

        return aqs.getAclsForSecurableObject( key );
    }

    @Timed
    @Override
    public Stream<AclKey> getAuthorizedObjects( Principal principal, EnumSet<Permission> permissions ) {
        return aqs.getAuthorizedAclKeys( principal, permissions );
    }

    @Timed
    @Override
    public Stream<AclKey> getAuthorizedObjects( Set<Principal> principal, EnumSet<Permission> permissions ) {
        return aqs.getAuthorizedAclKeys( principal, permissions );
    }

    @Timed
    @Override
    public Iterable<Principal> getSecurableObjectOwners( AclKey key ) {
        return aqs.getOwnersForSecurableObject( key );
    }

    @Timed
    @Override
    public Map<AceKey, AceValue> getPermissionMap( Set<AclKey> aclKeys, Set<Principal> principals ) {
        Set<AceKey> aceKeys = aclKeys
                .stream()
                .flatMap( aclKey -> principals.stream().map( p -> new AceKey( aclKey, p ) ) )
                .collect( Collectors.toSet() );

        return aces.getAll( aceKeys );
    }

    private static EnumMap<Permission, Boolean> noAccess( EnumSet<Permission> permissions ) {
        EnumMap<Permission, Boolean> pm = new EnumMap<Permission, Boolean>( Permission.class );
        for ( Permission p : permissions ) {
            pm.put( p, false );
        }
        return pm;
    }

    private static Predicate matches( Collection<AclKey> aclKeys, Set<Principal> principals ) {
        return Predicates.and( hasAnyAclKeys( aclKeys ), hasAnyPrincipals( principals ) );
    }

    private static Predicate hasExactPermissions( EnumSet<Permission> permissions ) {
        Predicate[] subPredicates = new Predicate[ permissions.size() ];
        int i = 0;
        for ( Permission p : permissions ) {
            subPredicates[ i++ ] = Predicates.equal( PermissionMapstore.PERMISSIONS_INDEX, p );
        }
        return Predicates.and( subPredicates );
    }

    private static Predicate hasAnyPermissions( EnumSet<Permission> permissions ) {
        return Predicates.in( PermissionMapstore.PERMISSIONS_INDEX, permissions.toArray( new Permission[ 0 ] ) );
    }

    private static Predicate hasExactAclKeys( Collection<AclKey> aclKeys ) {
        Predicate[] subPredicates = new Predicate[ aclKeys.size() ];
        int i = 0;
        for ( AclKey aclKey : aclKeys ) {
            subPredicates[ i++ ] = Predicates.equal( ACL_KEY_INDEX, aclKey );
        }
        return Predicates.and( subPredicates );
    }

    private static Predicate hasExactPrincipals( Collection<Principal> principals ) {
        Predicate[] subPredicates = new Predicate[ principals.size() ];
        int i = 0;
        for ( Principal principal : principals ) {
            subPredicates[ i++ ] = Predicates.equal( PermissionMapstore.PRINCIPAL_INDEX, principal );
        }
        return Predicates.and( subPredicates );
    }

    private static Predicate hasAnyPrincipals( Collection<Principal> principals ) {
        return Predicates.in( PermissionMapstore.PRINCIPAL_INDEX, principals.toArray( new Principal[ 0 ] ) );
    }

    private static Predicate hasAnyAclKeys( Collection<AclKey> aclKeys ) {
        //        String[] values = new AclKey[ aclKeys.size() ];
        //        int i = 0;
        //        for ( AclKey aclKey : aclKeys ) {
        //            values[ i++ ] = aclKey.getIndex();
        //        }
        return Predicates.in( ACL_KEY_INDEX, aclKeys.stream().map( AclKey::getIndex ).toArray( String[]::new ) );
    }

    private static Predicate hasAclKey( AclKey aclKey ) {
        return Predicates.equal( ACL_KEY_INDEX, aclKey.getIndex() );
    }

    private static Predicate hasType( SecurableObjectType objectType ) {
        return Predicates.equal( PermissionMapstore.SECURABLE_OBJECT_TYPE_INDEX, objectType );
    }

    private static Predicate hasPrincipal( Principal principal ) {
        return Predicates.equal( PermissionMapstore.PRINCIPAL_INDEX, principal );
    }

}
