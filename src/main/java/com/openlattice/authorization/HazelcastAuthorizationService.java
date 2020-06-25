

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

import com.codahale.metrics.annotation.Timed;
import com.dataloom.streams.StreamUtil;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.google.common.eventbus.EventBus;
import com.hazelcast.aggregation.Aggregators;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.openlattice.assembler.events.MaterializePermissionChangeEvent;
import com.openlattice.authorization.aggregators.AuthorizationSetAggregator;
import com.openlattice.authorization.aggregators.PrincipalAggregator;
import com.openlattice.authorization.mapstores.PermissionMapstore;
import com.openlattice.authorization.processors.AuthorizationEntryProcessor;
import com.openlattice.authorization.processors.PermissionMerger;
import com.openlattice.authorization.processors.PermissionRemover;
import com.openlattice.authorization.processors.SecurableObjectTypeUpdater;
import com.openlattice.authorization.securable.SecurableObjectType;
import com.openlattice.hazelcast.HazelcastMap;
import com.openlattice.organizations.PrincipalSet;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.OffsetDateTime;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Maps.transformValues;
import static com.openlattice.authorization.mapstores.PermissionMapstore.ACL_KEY_INDEX;
import static com.openlattice.authorization.util.AuthorizationUtilsKt.toAceKeys;

public class HazelcastAuthorizationService implements AuthorizationManager {
    private static final Logger logger = LoggerFactory.getLogger( AuthorizationManager.class );

    private final IMap<AclKey, SecurableObjectType> securableObjectTypes;
    private final IMap<AceKey, AceValue>            aces;
    private final EventBus                          eventBus;

    public HazelcastAuthorizationService(
            HazelcastInstance hazelcastInstance,
            EventBus eventBus ) {
        this.aces = HazelcastMap.PERMISSIONS.getMap( hazelcastInstance );
        this.securableObjectTypes = HazelcastMap.SECURABLE_OBJECT_TYPES.getMap( hazelcastInstance );
        this.eventBus = checkNotNull( eventBus );
    }

    @Override
    public void setSecurableObjectTypes( Set<AclKey> aclKeys, SecurableObjectType objectType ) {
        securableObjectTypes.putAll( Maps.asMap( aclKeys, k -> objectType ) );
        aces.executeOnEntries( new SecurableObjectTypeUpdater( objectType ), hasAnyAclKeys( aclKeys ) );
    }

    @Override
    public void setSecurableObjectType( AclKey aclKey, SecurableObjectType objectType ) {
        securableObjectTypes.set( aclKey, objectType );
        aces.executeOnEntries( new SecurableObjectTypeUpdater( objectType ), hasAclKey( aclKey ) );
    }

    private void signalMaterializationPermissionChange(
            AclKey key,
            Principal principal,
            EnumSet<Permission> permissions,
            SecurableObjectType securableObjectType
    ) {
        // if there is a change in materialization permission for a property type or an entity set for an organization
        // principal, we need to flag it
        if ( permissions.contains( Permission.MATERIALIZE )
                && principal.getType().equals( PrincipalType.ORGANIZATION )
                && ( securableObjectType.equals( SecurableObjectType.PropertyTypeInEntitySet )
                || securableObjectType.equals( SecurableObjectType.EntitySet ) ) ) {
            eventBus.post(
                    new MaterializePermissionChangeEvent( principal, Set.of( key.get( 0 ) ), securableObjectType )
            );
        }
    }

    @Override
    public void addPermission(
            AclKey key,
            Principal principal,
            EnumSet<Permission> permissions ) {
        addPermission( key, principal, permissions, OffsetDateTime.MAX );
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
        signalMaterializationPermissionChange( key, principal, permissions, securableObjectType );
    }

    @Override
    public void addPermissions(
            Set<AclKey> keys,
            Principal principal,
            EnumSet<Permission> permissions,
            SecurableObjectType securableObjectType ) {
        addPermissions( keys, principal, permissions, securableObjectType, OffsetDateTime.MAX );
    }

    @Override
    public void addPermissions(
            Set<AclKey> keys,
            Principal principal,
            EnumSet<Permission> permissions,
            SecurableObjectType securableObjectType,
            OffsetDateTime expirationDate ) {
        final var aceKeys = toAceKeys( keys, principal );
        aces.executeOnKeys( aceKeys, new PermissionMerger( permissions, securableObjectType, expirationDate ) );
    }

    @Override
    public void addPermissions( List<Acl> acls ) {
        SetMultimap<AceValue, AceKey> updates = getAceValueToAceKeyMap( acls );

        updates.keySet().forEach( aceValue -> {
            final var permissions = aceValue.getPermissions();
            final var securableObjectType = aceValue.getSecurableObjectType();
            final var expirationDate = aceValue.getExpirationDate();

            Set<AceKey> aceKeys = updates.get( aceValue );
            aces.executeOnKeys( aceKeys, new PermissionMerger( permissions, securableObjectType, expirationDate ) );
        } );
    }

    @Override
    public void removePermissions( List<Acl> acls ) {
        acls.stream().map( acl -> Pair.of( new AclKey( acl.getAclKey() ),
                StreamUtil.stream( acl.getAces() ).filter( ace -> ace.getPermissions().contains( Permission.OWNER ) )
                        .map( Ace::getPrincipal ).collect( Collectors.toSet() ) ) )
                .filter( pair -> !pair.getValue().isEmpty() )
                .forEach( pair ->
                        ensureAclKeysHaveOtherUserOwners( ImmutableSet.of( pair.getKey() ), pair.getValue() )
                );

        SetMultimap<AceValue, AceKey> updates = getAceValueToAceKeyMap( acls );

        updates.keySet().forEach( aceValue -> {
            final var permissions = aceValue.getPermissions();

            Set<AceKey> aceKeys = updates.get( aceValue );
            aces.executeOnKeys( aceKeys, new PermissionRemover( permissions ) );
        } );
    }

    private SetMultimap<AceValue, AceKey> getAceValueToAceKeyMap( List<Acl> acls ) {
        Map<AclKey, SecurableObjectType> types = securableObjectTypes
                .getAll( acls.stream().map( acl -> new AclKey( acl.getAclKey() ) ).collect( Collectors.toSet() ) );

        SetMultimap<AceValue, AceKey> map = HashMultimap.create();

        acls.forEach( acl -> {
            AclKey aclKey = new AclKey( acl.getAclKey() );
            SecurableObjectType securableObjectType = types.getOrDefault( aclKey, SecurableObjectType.Unknown );

            if ( securableObjectType.equals( SecurableObjectType.Unknown ) ) {
                logger.warn( "Unrecognized object type for acl key {} key ", aclKey );
            }

            acl.getAces().forEach( ace ->
                    map.put( new AceValue( ace.getPermissions(), securableObjectType, ace.getExpirationDate() ),
                            new AceKey( aclKey, ace.getPrincipal() ) )
            );
        } );

        return map;
    }

    @Override
    public void setPermissions( List<Acl> acls ) {
        Map<AclKey, SecurableObjectType> types = securableObjectTypes
                .getAll( acls.stream().map( acl -> new AclKey( acl.getAclKey() ) ).collect( Collectors.toSet() ) );

        Map<AceKey, AceValue> updates = Maps.newHashMap();

        acls.forEach( acl -> {
            AclKey aclKey = new AclKey( acl.getAclKey() );
            SecurableObjectType securableObjectType = types.getOrDefault( aclKey, SecurableObjectType.Unknown );

            if ( securableObjectType.equals( SecurableObjectType.Unknown ) ) {
                logger.warn( "Unrecognized object type for acl key {} key ", aclKey );
            }

            acl.getAces().forEach( ace -> {
                final var principal = ace.getPrincipal();
                final var permissions = ace.getPermissions();

                updates.put(
                        new AceKey( aclKey, principal ),
                        new AceValue( permissions, securableObjectType, ace.getExpirationDate() )
                );
                signalMaterializationPermissionChange( aclKey, principal, permissions, securableObjectType );
            } );
        } );

        aces.putAll( updates );
    }

    private void ensureAclKeysHaveOtherUserOwners( Set<AclKey> aclKeys, Set<Principal> principals ) {
        Set<Principal> userPrincipals = principals.stream().filter( p -> p.getType().equals( PrincipalType.USER ) )
                .collect( Collectors.toSet() );

        if ( userPrincipals.size() > 0 ) {

            Predicate allOtherUserOwnersPredicate = Predicates.and( hasAnyAclKeys( aclKeys ),
                    hasExactPermissions( EnumSet.of( Permission.OWNER ) ),
                    Predicates.not( hasAnyPrincipals( userPrincipals ) ),
                    hasPrincipalType( PrincipalType.USER ) );

            long allOtherUserOwnersCount = aces.aggregate( Aggregators.count(), allOtherUserOwnersPredicate );

            if ( allOtherUserOwnersCount == 0 ) {
                throw new IllegalStateException(
                        "Unable to remove owner permissions as a securable object will be left without an owner of " +
                                "type USER" );
            }
        }
    }

    @Override
    public void removePermission(
            AclKey key,
            Principal principal,
            EnumSet<Permission> permissions ) {
        if ( permissions.contains( Permission.OWNER ) ) {
            ensureAclKeysHaveOtherUserOwners( ImmutableSet.of( key ), ImmutableSet.of( principal ) );
        }
        signalMaterializationPermissionChange(
                key, principal, permissions, securableObjectTypes.getOrDefault( key, SecurableObjectType.Unknown )
        );
        aces.executeOnKey( new AceKey( key, principal ), new PermissionRemover( permissions ) );
    }

    @Override
    public void setPermission(
            AclKey key,
            Principal principal,
            EnumSet<Permission> permissions ) {
        setPermission( key, principal, permissions, OffsetDateTime.MAX );
    }

    @Override
    public void setPermission(
            AclKey key,
            Principal principal,
            EnumSet<Permission> permissions,
            OffsetDateTime expirationDate ) {
        if ( !permissions.contains( Permission.OWNER ) ) {
            ensureAclKeysHaveOtherUserOwners( ImmutableSet.of( key ), ImmutableSet.of( principal ) );
        }
        //This should be a rare call to overwrite all permissions, so it's okay to do a read before write.
        SecurableObjectType securableObjectType = securableObjectTypes.getOrDefault( key, SecurableObjectType.Unknown );
        signalMaterializationPermissionChange( key, principal, permissions, securableObjectType );
        aces.set( new AceKey( key, principal ), new AceValue( permissions, securableObjectType, expirationDate ) );
    }

    @Override
    public void setPermission( Set<AclKey> aclKeys, Set<Principal> principals, EnumSet<Permission> permissions ) {
        //This should be a rare call to overwrite all permissions, so it's okay to do a read before write.
        if ( !permissions.contains( Permission.OWNER ) ) {
            ensureAclKeysHaveOtherUserOwners( aclKeys, principals );
        }

        Map<AclKey, SecurableObjectType> securableObjectTypesForAclKeys = securableObjectTypes.getAll( aclKeys );

        Map<AceKey, AceValue> newPermissions = new HashMap<>( aclKeys.size() * principals.size() );
        for ( AclKey aclKey : aclKeys ) {
            SecurableObjectType objectType = securableObjectTypesForAclKeys
                    .getOrDefault( aclKey, SecurableObjectType.Unknown );

            AceValue aceValue = new AceValue( permissions, objectType, OffsetDateTime.MAX );

            for ( Principal principal : principals ) {
                newPermissions.put( new AceKey( aclKey, principal ), aceValue );
                signalMaterializationPermissionChange( aclKey, principal, permissions, objectType );
            }
        }

        aces.putAll( newPermissions );
    }

    @Override
    public void setPermissions( Map<AceKey, EnumSet<Permission>> permissions ) {

        permissions.entrySet().stream().filter( entry -> !entry.getValue().contains( Permission.OWNER ) )
                .collect( Collectors.groupingBy( e -> e.getKey().getAclKey(),
                        Collectors.mapping( e -> e.getKey().getPrincipal(), Collectors.toSet() ) ) )
                .forEach( ( aclKey, principals ) ->
                        ensureAclKeysHaveOtherUserOwners( ImmutableSet.of( aclKey ), principals ) );

        Map<AclKey, SecurableObjectType> securableObjectTypesForAclKeys = securableObjectTypes
                .getAll( permissions.keySet().stream().map( AceKey::getAclKey ).collect(
                        Collectors.toSet() ) );

        Map<AceKey, AceValue> newPermissions = Maps.newHashMap();

        permissions.forEach( ( aceKey, acePermissions ) -> {
            final var aclKey = aceKey.getAclKey();
            final var objectType = securableObjectTypesForAclKeys.getOrDefault( aclKey, SecurableObjectType.Unknown );

            newPermissions.put( aceKey, new AceValue( acePermissions, objectType, OffsetDateTime.MAX ) );

            signalMaterializationPermissionChange( aclKey, aceKey.getPrincipal(), acePermissions, objectType );
        } );

        aces.putAll( newPermissions );
    }

    @Override
    public void deletePermissions( AclKey aclKey ) {
        securableObjectTypes.delete( aclKey );
        aces.removeAll( hasAclKey( aclKey ) );
    }

    @Timed
    @Override
    public Map<AclKey, EnumMap<Permission, Boolean>> authorize(
            Map<AclKey, EnumSet<Permission>> requests,
            Set<Principal> principals ) {

        Map<AclKey, EnumMap<Permission, Boolean>> permissionMap = Maps.newHashMap( transformValues( requests,
                HazelcastAuthorizationService::noAccess ) );

        Set<AceKey> aceKeys = Sets.newHashSetWithExpectedSize( requests.size() * principals.size() );
        requests.keySet().forEach( aclKey ->
                principals.forEach( principal ->
                        aceKeys.add( new AceKey( aclKey, principal ) )
                )
        );

        aces.executeOnKeys( aceKeys, new AuthorizationEntryProcessor() ).forEach( ( aceKey, permissions ) -> {

            EnumMap<Permission, Boolean> aclKeyPermissions = permissionMap.get( aceKey.getAclKey() );

            ( (DelegatedPermissionEnumSet) permissions ).forEach( ( p ) -> {
                if ( aclKeyPermissions.containsKey( p ) ) {
                    aclKeyPermissions.put( p, true );
                }
            } );

            permissionMap.put( aceKey.getAclKey(), aclKeyPermissions );
        } );

        return permissionMap;
    }

    @Timed
    @Override
    public Stream<Authorization> accessChecksForPrincipals(
            Set<AccessCheck> accessChecks,
            Set<Principal> principals ) {

        Map<AclKey, EnumSet<Permission>> requests = Maps.newLinkedHashMapWithExpectedSize( accessChecks.size() );
        accessChecks.forEach( ac -> {
            EnumSet<Permission> p = requests.getOrDefault( ac.getAclKey(), EnumSet.noneOf( Permission.class ) );
            p.addAll( ac.getPermissions() );
            requests.put( ac.getAclKey(), p );
        } );

        return authorize( requests, principals )
                .entrySet()
                .stream()
                .map( e -> new Authorization( e.getKey(), e.getValue() ) );
    }

    @Override
    public void deletePrincipalPermissions( Principal principal ) {
        aces.removeAll( hasPrincipal( principal ) );
    }

    public Predicate matches( AclKey aclKey, Collection<Principal> principals ) {
        return Predicates.and(
                hasAclKey( aclKey ),
                hasAnyPrincipals( principals ) );
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

        EnumSet<Permission> actualPermissions = EnumSet.noneOf( Permission.class );

        Set<AceKey> aceKeys = Sets.newHashSetWithExpectedSize( principals.size() );
        principals.forEach( p -> aceKeys.add( new AceKey( key, p ) ) );

        aces.executeOnKeys( aceKeys, new AuthorizationEntryProcessor() ).values().forEach( pSet ->
                actualPermissions.addAll( (DelegatedPermissionEnumSet) pSet )
        );

        return actualPermissions.containsAll( requiredPermissions );
    }

    @Timed
    @Override
    public Map<Set<AclKey>, EnumSet<Permission>> getSecurableObjectSetsPermissions(
            Collection<Set<AclKey>> aclKeySets,
            Set<Principal> principals ) {

        return aclKeySets.parallelStream().collect( Collectors.toMap(
                Function.identity(),
                aclKeySet -> getSecurableObjectSetPermissions( aclKeySet, principals )
        ) );
    }

    private EnumSet<Permission> getSecurableObjectSetPermissions(
            Set<AclKey> aclKeySet,
            Set<Principal> principals ) {

        var authorizationsMap = aclKeySet.stream()
                .collect( Collectors.toMap( Function.identity(), aclKey -> EnumSet.noneOf( Permission.class ) ) );

        return aces.aggregate(
                new AuthorizationSetAggregator( authorizationsMap ),
                matches( aclKeySet, principals ) );
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
                //                .peek( ps -> logger.info( "Implementing class: {}", ps.getClass().getCanonicalName
                //                () ) )
                .map( AceValue::getPermissions )
                .filter( Objects::nonNull )
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
    public Stream<AclKey> getAuthorizedObjectsOfType(
            Set<Principal> principals,
            SecurableObjectType objectType,
            EnumSet<Permission> permissions,
            Predicate additionalFilter ) {
        Predicate p = Predicates
                .and( hasAnyPrincipals( principals ),
                        hasType( objectType ),
                        hasExactPermissions( permissions ),
                        additionalFilter );
        return this.aces.keySet( p )
                .stream()
                .map( AceKey::getAclKey )
                .distinct();
    }

    @Timed
    @Override
    public Acl getAllSecurableObjectPermissions( AclKey key ) {
        var permissionEntries = aces.entrySet( hasAclKey( key ) );
        Set<Ace> acesWithPermissions = Sets.newHashSetWithExpectedSize( permissionEntries.size() );

        permissionEntries.forEach( entry ->
                acesWithPermissions.add( new Ace( entry.getKey().getPrincipal(), entry.getValue().getPermissions() ) )
        );

        return new Acl( key, acesWithPermissions );
    }

    @Override
    public Set<Principal> getAuthorizedPrincipalsOnSecurableObject( AclKey key, EnumSet<Permission> permissions ) {
        final Map<AclKey, PrincipalSet> principalMap = new HashMap<>();
        principalMap.put( key, new PrincipalSet( new HashSet<>() ) );

        PrincipalAggregator agg = aces.aggregate(
                new PrincipalAggregator( ( principalMap ) ), matches( key, permissions ) );

        return agg.getResult().get( key );
    }

    @Timed
    @Override
    public Set<Principal> getSecurableObjectOwners( AclKey key ) {
        return getAuthorizedPrincipalsOnSecurableObject( key, EnumSet.of( Permission.OWNER ) );
    }

    @Timed
    @Override
    public SetMultimap<AclKey, Principal> getOwnersForSecurableObjects( Collection<AclKey> aclKeys ) {
        SetMultimap<AclKey, Principal> result = HashMultimap.create();

        aces.keySet( Predicates.and( hasAnyAclKeys( aclKeys ), hasExactPermissions( EnumSet.of( Permission.OWNER ) ) ) )
                .forEach( aceKey -> result.put( aceKey.getAclKey(), aceKey.getPrincipal() ) );

        return result;
    }

    private static EnumMap<Permission, Boolean> noAccess( EnumSet<Permission> permissions ) {
        EnumMap<Permission, Boolean> pm = new EnumMap<>( Permission.class );
        for ( Permission p : permissions ) {
            pm.put( p, false );
        }
        return pm;
    }

    private static Predicate matches( Collection<AclKey> aclKeys, Set<Principal> principals ) {
        return Predicates.and( hasAnyAclKeys( aclKeys ), hasAnyPrincipals( principals ) );
    }

    private static Predicate matches( AclKey aclKey, EnumSet<Permission> permissions ) {
        return Predicates.and( hasAclKey( aclKey ), hasExactPermissions( permissions ) );
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
        //        String[] values = new AclKey[ aclKey.size() ];
        //        int i = 0;
        //        for ( AclKey aclKey : aclKey ) {
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

    private static Predicate hasPrincipalType( PrincipalType type ) {
        return Predicates.equal( PermissionMapstore.PRINCIPAL_TYPE_INDEX, type );
    }

}
