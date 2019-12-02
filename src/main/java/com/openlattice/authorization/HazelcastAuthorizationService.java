

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
import com.dataloom.streams.StreamUtil;
import com.google.common.base.Stopwatch;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;
import com.google.common.eventbus.EventBus;
import com.hazelcast.aggregation.Aggregators;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.openlattice.assembler.events.MaterializePermissionChangeEvent;
import com.openlattice.authorization.aggregators.AuthorizationAggregator;
import com.openlattice.authorization.aggregators.AuthorizationSetAggregator;
import com.openlattice.authorization.aggregators.PrincipalAggregator;
import com.openlattice.authorization.mapstores.PermissionMapstore;
import com.openlattice.authorization.paging.AuthorizedObjectsSearchResult;
import com.openlattice.authorization.processors.PermissionMerger;
import com.openlattice.authorization.processors.PermissionRemover;
import com.openlattice.authorization.processors.SecurableObjectTypeUpdater;
import com.openlattice.authorization.securable.SecurableObjectType;
import com.openlattice.hazelcast.HazelcastMap;
import com.openlattice.organizations.PrincipalSet;
import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.Pair;
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

    private void signalMaterializationPermissionChangeBulk( SetMultimap<AceValue, AceKey> updates ) {
        // if there is a change in materialization permission for a property type or an entity set for an organization
        // principal, we need to flag it

        // filter entries relevant for materialize permission changes
        updates.entries().stream()
                .filter(
                        aceEntry -> ( aceEntry.getKey().getSecurableObjectType().equals( SecurableObjectType.EntitySet )
                                || aceEntry.getKey().getSecurableObjectType()
                                .equals( SecurableObjectType.PropertyTypeInEntitySet ) )
                                && ( aceEntry.getKey().getPermissions().contains( Permission.MATERIALIZE )
                                && ( aceEntry.getValue().getPrincipal().getType()
                                .equals( PrincipalType.ORGANIZATION ) ) )
                )
                // group by organization (principal)
                .collect( Collectors.groupingBy( aceEntry -> aceEntry.getValue().getPrincipal() ) )
                .forEach( ( principal, aceEntries ) ->
                        aceEntries.stream()
                                // group by object type
                                .collect( Collectors
                                        .groupingBy( aceEnrty -> aceEnrty.getKey().getSecurableObjectType() ) )
                                .forEach( ( securableObjectType, aceEntriesOfType ) -> {
                                            final var entitySetIds = aceEntriesOfType.stream()
                                                    .map( aceEntryOfType -> aceEntryOfType.getValue().getAclKey().get( 0 ) )
                                                    .collect( Collectors.toSet() );
                                            eventBus.post(
                                                    new MaterializePermissionChangeEvent(
                                                            principal,
                                                            entitySetIds,
                                                            securableObjectType )
                                            );
                                        }
                                )
                );

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
        signalMaterializationPermissionChange( key, principal, permissions, securableObjectType );
        aces.executeOnKey( new AceKey( key, principal ),
                new PermissionMerger( permissions, securableObjectType, expirationDate ) );
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

        signalMaterializationPermissionChangeBulk( updates );
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
        signalMaterializationPermissionChangeBulk( updates );
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
    public SetMultimap<AclKey, Principal> getOwnersForSecurableObjects( Collection<AclKey> aclKeys ) {
        SetMultimap<AclKey, Principal> result = HashMultimap.create();

        aces.keySet( Predicates.and( hasAnyAclKeys( aclKeys ), hasExactPermissions( EnumSet.of( Permission.OWNER ) ) ) )
                .forEach( aceKey -> result.put( aceKey.getAclKey(), aceKey.getPrincipal() ) );

        return result;
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
