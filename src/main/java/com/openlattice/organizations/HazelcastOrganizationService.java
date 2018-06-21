

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

package com.openlattice.organizations;

import static com.google.common.base.Preconditions.checkNotNull;

import com.dataloom.streams.StreamUtil;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.eventbus.EventBus;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.openlattice.authorization.AclKey;
import com.openlattice.authorization.AuthorizationManager;
import com.openlattice.authorization.HazelcastAclKeyReservationService;
import com.openlattice.authorization.Permission;
import com.openlattice.authorization.Principal;
import com.openlattice.authorization.PrincipalType;
import com.openlattice.authorization.SecurablePrincipal;
import com.openlattice.datastore.util.Util;
import com.openlattice.directory.UserDirectoryService;
import com.openlattice.hazelcast.HazelcastMap;
import com.openlattice.organization.Organization;
import com.openlattice.organization.OrganizationPrincipal;
import com.openlattice.organization.roles.Role;
import com.openlattice.organizations.events.OrganizationCreatedEvent;
import com.openlattice.organizations.events.OrganizationDeletedEvent;
import com.openlattice.organizations.events.OrganizationUpdatedEvent;
import com.openlattice.organizations.processors.EmailDomainsMerger;
import com.openlattice.organizations.processors.EmailDomainsRemover;
import com.openlattice.organizations.processors.OrganizationAppMerger;
import com.openlattice.organizations.processors.OrganizationAppRemover;
import com.openlattice.organizations.processors.OrganizationMemberMerger;
import com.openlattice.organizations.processors.OrganizationMemberRemover;
import com.openlattice.organizations.roles.SecurePrincipalsManager;
import com.openlattice.rhizome.hazelcast.DelegatedStringSet;
import com.openlattice.rhizome.hazelcast.DelegatedUUIDSet;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HazelcastOrganizationService {

    private static final Logger logger = LoggerFactory
            .getLogger( HazelcastOrganizationService.class );
    private final AuthorizationManager              authorizations;
    private final HazelcastAclKeyReservationService reservations;
    private final UserDirectoryService              principals;
    private final SecurePrincipalsManager           securePrincipalsManager;
    private final IMap<UUID, String>                titles;
    private final IMap<UUID, String>                descriptions;
    private final IMap<UUID, DelegatedStringSet>    autoApprovedEmailDomainsOf;
    private final IMap<UUID, PrincipalSet>          membersOf;
    private final IMap<UUID, DelegatedUUIDSet>      apps;
    private final List<IMap<UUID, ?>>               allMaps;

    @Inject
    private EventBus eventBus;

    public HazelcastOrganizationService(
            HazelcastInstance hazelcastInstance,
            HazelcastAclKeyReservationService reservations,
            AuthorizationManager authorizations,
            UserDirectoryService principals,
            SecurePrincipalsManager securePrincipalsManager ) {
        this.titles = hazelcastInstance.getMap( HazelcastMap.ORGANIZATIONS_TITLES.name() );
        this.descriptions = hazelcastInstance.getMap( HazelcastMap.ORGANIZATIONS_DESCRIPTIONS.name() );
        this.autoApprovedEmailDomainsOf = hazelcastInstance.getMap( HazelcastMap.ALLOWED_EMAIL_DOMAINS.name() );
        this.membersOf = hazelcastInstance.getMap( HazelcastMap.ORGANIZATIONS_MEMBERS.name() );
        this.apps = hazelcastInstance.getMap( HazelcastMap.ORGANIZATION_APPS.name() );
        this.authorizations = authorizations;
        this.reservations = reservations;
        this.allMaps = ImmutableList.of( titles,
                descriptions,
                autoApprovedEmailDomainsOf,
                membersOf,
                apps );
        this.principals = checkNotNull( principals );
        this.securePrincipalsManager = securePrincipalsManager;
    }

    public void createOrganization( Principal principal, Organization organization ) {
        securePrincipalsManager.createSecurablePrincipalIfNotExists( principal, organization.getSecurablePrincipal() );
        createOrganization( organization );
        eventBus.post( new OrganizationCreatedEvent( organization ) );
    }

    public void createOrganization( Organization organization ) {
        UUID organizationId = organization.getSecurablePrincipal().getId();
        titles.set( organizationId, organization.getTitle() );
        descriptions.set( organizationId, organization.getDescription() );
        autoApprovedEmailDomainsOf.set( organizationId,
                DelegatedStringSet.wrap( organization.getAutoApprovedEmails() ) );
        membersOf.set( organizationId, PrincipalSet.wrap( organization.getMembers() ) );
        apps.set( organizationId, DelegatedUUIDSet.wrap( organization.getApps() ) );
    }

    public Organization getOrganization( UUID organizationId ) {
        Future<PrincipalSet> members = membersOf.getAsync( organizationId );
        Future<DelegatedStringSet> autoApprovedEmailDomains = autoApprovedEmailDomainsOf.getAsync( organizationId );

        Collection<SecurablePrincipal> maybeOrgs =
                securePrincipalsManager.getSecurablePrincipals( getOrganizationPredicate( organizationId ) );
        if ( maybeOrgs.isEmpty() ) {
            logger.error( "Organization id {} has no corresponding securable principal.", organizationId );
            return null;
        }

        OrganizationPrincipal principal = (OrganizationPrincipal) Iterables.getOnlyElement( maybeOrgs );
        Set<Role> roles = getRoles( organizationId );
        Set<UUID> apps = getOrganizationApps( organizationId );
        try {
            return new Organization(
                    principal,
                    autoApprovedEmailDomains.get(),
                    members.get(),
                    roles,
                    apps );
        } catch ( InterruptedException | ExecutionException e ) {
            logger.error( "Unable to load organization. {}", organizationId, e );
            return null;
        }
    }

    public void destroyOrganization( UUID organizationId ) {
        // Remove all roles
        securePrincipalsManager.deleteAllRolesInOrganization( organizationId );
        allMaps.stream().forEach( m -> m.delete( organizationId ) );
        reservations.release( organizationId );
        eventBus.post( new OrganizationDeletedEvent( organizationId ) );
    }

    public void updateTitle( UUID organizationId, String title ) {
        securePrincipalsManager.updateTitle( new AclKey( organizationId ), title );
        eventBus.post( new OrganizationUpdatedEvent( organizationId, Optional.of( title ), Optional.empty() ) );
    }

    public void updateDescription( UUID organizationId, String description ) {
        securePrincipalsManager.updateDescription( new AclKey( organizationId ), description );
        eventBus.post( new OrganizationUpdatedEvent( organizationId, Optional.empty(), Optional.of( description ) ) );
    }

    public Set<String> getAutoApprovedEmailDomains( UUID organizationId ) {
        return Util.getSafely( autoApprovedEmailDomainsOf, organizationId );
    }

    public void setAutoApprovedEmailDomains( UUID organizationId, Set<String> emailDomains ) {
        autoApprovedEmailDomainsOf.set( organizationId, DelegatedStringSet.wrap( emailDomains ) );
    }

    public void addAutoApprovedEmailDomains( UUID organizationId, Set<String> emailDomains ) {
        autoApprovedEmailDomainsOf.submitToKey( organizationId, new EmailDomainsMerger( emailDomains ) );
    }

    public void removeAutoApprovedEmailDomains( UUID organizationId, Set<String> emailDomains ) {
        autoApprovedEmailDomainsOf.submitToKey( organizationId, new EmailDomainsRemover( emailDomains ) );
    }

    public Set<Principal> getMembers( UUID organizationId ) {
        return Util.getSafely( membersOf, organizationId );
    }

    public void addMembers( UUID organizationId, Set<Principal> members ) {
        membersOf.submitToKey( organizationId, new OrganizationMemberMerger( members ) );
        addOrganizationToMembers( organizationId, members );
        final AclKey orgAclKey = new AclKey( organizationId );
        members.stream().filter( PrincipalType.USER::equals )
                .map( securePrincipalsManager::lookup )
                .forEach( target -> securePrincipalsManager.addPrincipalToPrincipal( orgAclKey, target ) );
    }

    public void setMembers( UUID organizationId, Set<Principal> members ) {
        Set<Principal> current = Util.getSafely( membersOf, organizationId );
        Set<Principal> removed = current
                .stream()
                .filter( member -> !members.contains( member ) && current.contains( member ) )
                .collect( Collectors.toSet() );

        Set<Principal> added = current
                .stream()
                .filter( member -> members.contains( member ) && !current.contains( member ) )
                .collect( Collectors.toSet() );

        addMembers( organizationId, added );
        removeMembers( organizationId, removed );
    }

    public void removeMembers( UUID organizationId, Set<Principal> members ) {
        removeRolesFromMembers(
                getRolesInFull( organizationId ).stream().map( Role::getAclKey ),
                members
                        .stream()
                        .filter( m -> m.getType().equals( PrincipalType.USER ) )
                        .map( securePrincipalsManager::lookup ) );
        membersOf.submitToKey( organizationId, new OrganizationMemberRemover( members ) );
        removeOrganizationFromMembers( organizationId, members );

        final AclKey orgAclKey = new AclKey( organizationId );
        members.stream().filter( PrincipalType.USER::equals )
                .map( securePrincipalsManager::lookup )
                .forEach( target -> securePrincipalsManager.removePrincipalFromPrincipal( orgAclKey, target ) );
    }

    private void addOrganizationToMembers( UUID organizationId, Set<Principal> members ) {
        if ( members.stream().map( Principal::getType ).allMatch( PrincipalType.USER::equals ) ) {
            members.forEach( member -> principals.addOrganizationToUser( member.getId(), organizationId ) );
        } else {
            throw new IllegalArgumentException( "Cannot add a non-user role as a member of an organization." );
        }
    }

    private void removeOrganizationFromMembers( UUID organizationId, Set<Principal> members ) {
        if ( members.stream().map( Principal::getType ).allMatch( PrincipalType.USER::equals ) ) {
            members.forEach( member -> principals.removeOrganizationFromUser( member.getId(), organizationId ) );
        } else {
            throw new IllegalArgumentException( "Cannot add a non-user role as a member of an organization." );
        }
    }

    private void removeRolesFromMembers( Stream<AclKey> roles, Stream<AclKey> members ) {
        members.forEach( member -> roles
                .forEach( role -> securePrincipalsManager.removePrincipalFromPrincipal( role, member ) ) );
    }

    public void createRoleIfNotExists( Principal callingUser, Role role ) {
        securePrincipalsManager.createSecurablePrincipalIfNotExists( callingUser, role );
        final UUID organizationId = role.getOrganizationId();
        final SecurablePrincipal orgPrincipal = securePrincipalsManager
                .getSecurablePrincipal( new AclKey( organizationId ) );
        authorizations.addPermission( role.getAclKey(), orgPrincipal.getPrincipal(), EnumSet.of( Permission.READ ) );
    }

    private Collection<Role> getRolesInFull( UUID organizationId ) {
        return securePrincipalsManager.getAllRolesInOrganization( organizationId )
                .stream()
                .map( sp -> (Role) sp )
                .collect( Collectors.toList() );
    }

    public Set<Role> getRoles( UUID organizationId ) {
        return StreamUtil.stream( getRolesInFull( organizationId ) ).collect( Collectors.toSet() );
    }

    public void removeRoleFromUser( AclKey roleKey, Principal user ) {
        securePrincipalsManager.removePrincipalFromPrincipal( roleKey, securePrincipalsManager.lookup( user ) );
    }

    public void addAppToOrg( UUID organizationId, UUID appId ) {
        apps.executeOnKey( organizationId,
                new OrganizationAppMerger( DelegatedUUIDSet.wrap( ImmutableSet.of( appId ) ) ) );
    }

    public void removeAppFromOrg( UUID organizationId, UUID appId ) {
        apps.executeOnKey( organizationId,
                new OrganizationAppRemover( DelegatedUUIDSet.wrap( ImmutableSet.of( appId ) ) ) );
    }

    public Set<UUID> getOrganizationApps( UUID organizationId ) {
        return apps.get( organizationId );
    }

    private static Predicate getOrganizationPredicate( UUID organizationId ) {
        return Predicates.and(
                Predicates.equal( "principalType", PrincipalType.ORGANIZATION ),
                Predicates.equal( "aclKey[0]", organizationId ) );
    }
}
