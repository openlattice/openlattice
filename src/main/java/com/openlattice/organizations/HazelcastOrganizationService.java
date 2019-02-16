

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

import com.dataloom.streams.StreamUtil;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.eventbus.EventBus;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.openlattice.apps.AppConfigKey;
import com.openlattice.apps.AppTypeSetting;
import com.openlattice.assembler.Assembler;
import com.openlattice.assembler.AssemblerConnectionManager;
import com.openlattice.authorization.*;
import com.openlattice.authorization.securable.SecurableObjectType;
import com.openlattice.bootstrap.AuthorizationBootstrap;
import com.openlattice.datastore.util.Util;
import com.openlattice.directory.UserDirectoryService;
import com.openlattice.hazelcast.HazelcastMap;
import com.openlattice.organization.Organization;
import com.openlattice.organization.OrganizationIntegrationAccount;
import com.openlattice.organization.OrganizationPrincipal;
import com.openlattice.organization.roles.Role;
import com.openlattice.organizations.events.OrganizationCreatedEvent;
import com.openlattice.organizations.events.OrganizationDeletedEvent;
import com.openlattice.organizations.events.OrganizationUpdatedEvent;
import com.openlattice.organizations.processors.*;
import com.openlattice.organizations.roles.SecurePrincipalsManager;
import com.openlattice.postgres.mapstores.AppConfigMapstore;
import com.openlattice.rhizome.hazelcast.DelegatedStringSet;
import com.openlattice.rhizome.hazelcast.DelegatedUUIDSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * This class manages organizations.
 * <p>
 * An organization is a collection of principals and applications.
 * <p>
 * Access to organizations is handled by the organization manager.
 * <p>
 * Membership in an organization is stored in the membersOf field whcih is accessed via an IMAP. Only principals of type
 * {@link PrincipalType#USER}. This is mainly because we don't store the principal type field along with the principal id.
 * This may change in the future.
 * <p>
 * While roles may create organizations they cannot be members. That is an organization created by a role will have no members
 * but principals with that role will have the relevant level of access to that role. In addition, roles that create an
 * organization will not inherit the organization role (as they are not members).
 */
public class HazelcastOrganizationService {

    private static final Logger logger = LoggerFactory.getLogger( HazelcastOrganizationService.class );

    private final AuthorizationManager               authorizations;
    private final HazelcastAclKeyReservationService  reservations;
    private final UserDirectoryService               principals;
    private final SecurePrincipalsManager            securePrincipalsManager;
    private final IMap<UUID, String>                 titles;
    private final IMap<UUID, String>                 descriptions;
    private final IMap<UUID, DelegatedStringSet>     autoApprovedEmailDomainsOf;
    private final IMap<UUID, PrincipalSet>           membersOf;
    private final IMap<UUID, DelegatedUUIDSet>       apps;
    private final IMap<AppConfigKey, AppTypeSetting> appConfigs;
    private final List<IMap<UUID, ?>>                allMaps;
    private final Assembler                          assembler;

    @Inject
    private EventBus eventBus;

    public HazelcastOrganizationService(
            HazelcastInstance hazelcastInstance,
            HazelcastAclKeyReservationService reservations,
            AuthorizationManager authorizations,
            UserDirectoryService principals,
            SecurePrincipalsManager securePrincipalsManager,
            Assembler assembler ) {
        this.titles = hazelcastInstance.getMap( HazelcastMap.ORGANIZATIONS_TITLES.name() );
        this.descriptions = hazelcastInstance.getMap( HazelcastMap.ORGANIZATIONS_DESCRIPTIONS.name() );
        this.autoApprovedEmailDomainsOf = hazelcastInstance.getMap( HazelcastMap.ALLOWED_EMAIL_DOMAINS.name() );
        this.membersOf = hazelcastInstance.getMap( HazelcastMap.ORGANIZATIONS_MEMBERS.name() );
        this.apps = hazelcastInstance.getMap( HazelcastMap.ORGANIZATION_APPS.name() );
        this.appConfigs = hazelcastInstance.getMap( HazelcastMap.APP_CONFIGS.name() );
        this.authorizations = authorizations;
        this.reservations = reservations;
        this.allMaps = ImmutableList.of( titles,
                descriptions,
                autoApprovedEmailDomainsOf,
                membersOf,
                apps );
        this.principals = checkNotNull( principals );
        this.securePrincipalsManager = securePrincipalsManager;
        this.assembler = assembler;
        //        fixOrganizations();
    }

    public OrganizationPrincipal getOrganization( Principal p ) {
        OrganizationPrincipal organizationPrincipal = (OrganizationPrincipal) securePrincipalsManager
                .getPrincipal( p.getId() );
        return checkNotNull( organizationPrincipal );
    }

    public Optional<SecurablePrincipal> maybeGetOrganization( Principal p ) {
        return securePrincipalsManager.maybeGetSecurablePrincipal( p );
    }

    public void createOrganization( Principal principal, Organization organization ) {
        checkState( securePrincipalsManager
                        .createSecurablePrincipalIfNotExists( principal, organization.getSecurablePrincipal() ),
                "Unable to create securable principal for organization. This means the organization probably already exists." );
        createOrganization( organization );

        //Create the admin role for the organization and give it ownership of organization.
        var adminRole = createOrganizationAdminRole( organization.getSecurablePrincipal() );
        createRoleIfNotExists( principal, adminRole );
        authorizations
                .addPermission( organization.getAclKey(), adminRole.getPrincipal(), EnumSet.allOf( Permission.class ) );

        /*
         * Roles shouldn't be members of an organizations.
         *
         * Membership is currently defined as your principal having the
         * organization principal.
         *
         * In order to function roles must have READ access on the organization and
         */

        switch ( principal.getType() ) {
            case USER:
                //Add the organization principal to the creator marking them as a member of the organization
                addMembers( organization.getAclKey(), ImmutableSet.of( principal ) );
                //Fall throught by design
                break;
            case ROLE:
                //For a role we ensure that it has
                logger.debug( "Creating an organization with no members, but accessible by {}", principal );
                break;
            default:
                throw new IllegalStateException( "Only users and roles can create organizations." );
        }

        //Grant the creator of the organizations
        authorizations.addPermission( organization.getAclKey(), principal, EnumSet.allOf( Permission.class ) );
        //We add the user/role that created the organization to the admin role for the organization
        addRoleToPrincipalInOrganization( organization.getId(), adminRole.getId(), principal );
        assembler.createOrganization( organization );
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

        OrganizationPrincipal principal = getOrganizationPrincipal( organizationId );

        if ( principal == null ) {
            return null;
        }

        Set<Role> roles = getRoles( organizationId );
        Set<UUID> apps = getOrganizationApps( organizationId );

        try {
            PrincipalSet orgMembers = members.get();
            if ( orgMembers == null ) {
                logger.error( "Encountered null principal set for organization: {}", organizationId );
            }
            if ( apps == null ) {
                logger.error( "Encounter null application: {}", organizationId );
            }
            return new Organization(
                    principal,
                    MoreObjects.firstNonNull( autoApprovedEmailDomains.get(), ImmutableSet.of() ),
                    MoreObjects.firstNonNull( orgMembers, ImmutableSet.of() ),
                    roles,
                    MoreObjects.firstNonNull( apps, ImmutableSet.of() ) );
        } catch ( InterruptedException | ExecutionException e ) {
            logger.error( "Unable to load organization. {}", organizationId, e );
            return null;
        }
    }

    public Iterable<Organization> getOrganizations( Stream<UUID> organizationIds ) {
        return organizationIds.map( this::getOrganization )
                .map( org -> new Organization(
                        org.getSecurablePrincipal(),
                        org.getAutoApprovedEmails(),
                        org.getMembers(),
                        //TODO: If you're an organization you can view its roles.
                        org.getRoles(),
                        org.getApps() ) )
                .filter( com.google.common.base.Predicates.notNull()::apply )
                ::iterator;
    }

    public void destroyOrganization( UUID organizationId ) {
        // Remove all roles
        var aclKey = new AclKey( organizationId );
        authorizations.deletePermissions( aclKey );
        securePrincipalsManager.deleteAllRolesInOrganization( organizationId );
        securePrincipalsManager.deletePrincipal( aclKey );
        allMaps.stream().forEach( m -> m.delete( organizationId ) );
        reservations.release( organizationId );
        appConfigs.removeAll( Predicates.equal( AppConfigMapstore.ORGANIZATION_ID, organizationId ) );
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
        addMembers( new AclKey( organizationId ), members );
    }

    public void addMembers( AclKey orgAclKey, Set<Principal> members ) {
        checkState( orgAclKey.size() == 1, "Organization acl key should only be of length 1" );
        checkState( members
                .stream()
                .peek( principal -> {
                    if ( !principal.getType().equals( PrincipalType.USER ) ) {
                        logger.info( "Attempting to add non-user principal {} to organization {}",
                                principal,
                                orgAclKey );
                    }
                } )
                .map( Principal::getType )
                .allMatch( PrincipalType.USER::equals ), "Can only add users to organizations." );
        var organizationId = orgAclKey.get( 0 );

        //Add members to member list.
        membersOf.submitToKey( organizationId, new OrganizationMemberMerger( members ) );

        //Add the organization principal to each user
        members.stream()
                //Grant read on the organization
                .peek( principal -> authorizations
                        .addPermission( orgAclKey, principal, EnumSet.of( Permission.READ ) ) )
                .map( securePrincipalsManager::lookup )
                //Assign organization principal.
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
        final UUID organizationId = role.getOrganizationId();
        final OrganizationPrincipal orgPrincipal = (OrganizationPrincipal) securePrincipalsManager
                .getSecurablePrincipal( new AclKey( organizationId ) );
        /*
         * We set the organization to be the owner of the principal and grant everyone in the organization read access
         * to the principal. This is done so that anyone in the organization can see the principal and the owners of
         * an organization all have owner on the principal
         */
        securePrincipalsManager.createSecurablePrincipalIfNotExists( callingUser, role );
        authorizations.addPermission( role.getAclKey(), orgPrincipal.getPrincipal(), EnumSet.of( Permission.READ ) );
        AssemblerConnectionManager.createRole( role );
    }

    public void addRoleToPrincipalInOrganization( UUID organizationId, UUID roleId, Principal principal ) {
        securePrincipalsManager.addPrincipalToPrincipal( new AclKey( organizationId, roleId ),
                securePrincipalsManager.lookup( principal ) );
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

    private void fixOrganizations() {
        checkNotNull( AuthorizationBootstrap.GLOBAL_ADMIN_ROLE.getPrincipal() );
        logger.info( "Fixing organizations." );
        for ( SecurablePrincipal organization : securePrincipalsManager
                .getSecurablePrincipals( PrincipalType.ORGANIZATION ) ) {
            authorizations.setSecurableObjectType( organization.getAclKey(), SecurableObjectType.Organization );
            authorizations.addPermission( organization.getAclKey(),
                    AuthorizationBootstrap.GLOBAL_ADMIN_ROLE.getPrincipal(),
                    EnumSet.allOf( Permission.class ) );

            logger.info( "Setting titles, descriptions, and autoApproved e-mails domains if not present." );
            titles.putIfAbsent( organization.getId(), organization.getTitle() );
            descriptions.putIfAbsent( organization.getId(), organization.getDescription() );
            autoApprovedEmailDomainsOf.putIfAbsent( organization.getId(), DelegatedStringSet.wrap( new HashSet<>() ) );
            apps.putIfAbsent( organization.getId(), DelegatedUUIDSet.wrap( new HashSet<>() ) );
            membersOf.putIfAbsent( organization.getId(), new PrincipalSet( ImmutableSet.of() ) );

            logger.info( "Synchronizing roles" );
            var roles = securePrincipalsManager.getAllRolesInOrganization( organization.getId() );
            //Grant the organization principal read permission on each principal
            for ( SecurablePrincipal role : roles ) {
                authorizations.setSecurableObjectType( role.getAclKey(), SecurableObjectType.Role );
                authorizations
                        .addPermission( role.getAclKey(), organization.getPrincipal(), EnumSet.of( Permission.READ ) );
            }

            logger.info( "Synchronizing members" );
            PrincipalSet principals = PrincipalSet.wrap( new HashSet<>( securePrincipalsManager
                    .getAllUsersWithPrincipal( organization.getAclKey() ) ) );
            //Add all users who have the organization role to the organizaton.
            addMembers( organization.getAclKey(), principals );

            /*
             * This is a one time thing so that admins at this point in time have access to and can fix organizations.
             *
             * For simplicity we are going to add all admin users into all organizations. We will have to manually clean
             * this up afterwards.
             */

            logger.info( "Synchronizing admins." );
            var adminPrincipals = PrincipalSet.wrap( securePrincipalsManager.getAllUsersWithPrincipal(
                    securePrincipalsManager.lookup( AuthorizationBootstrap.GLOBAL_ADMIN_ROLE.getPrincipal() ) )
                    .stream()
                    .collect( Collectors.toSet() ) );

            addMembers( organization.getAclKey(), adminPrincipals );
            adminPrincipals.forEach( admin -> authorizations
                    .addPermission( organization.getAclKey(), admin, EnumSet.allOf( Permission.class ) ) );

        }
    }

    public OrganizationPrincipal getOrganizationPrincipal( UUID organizationId ) {
        final var maybeOrganizationPrincipal = securePrincipalsManager
                .getSecurablePrincipals( getOrganizationPredicate( organizationId ) );
        if ( maybeOrganizationPrincipal.isEmpty() ) {
            logger.error( "Organization id {} has no corresponding securable principal.", organizationId );
            return null;
        }
        return (OrganizationPrincipal) Iterables.getOnlyElement( maybeOrganizationPrincipal );
    }

    public static Role createOrganizationAdminRole( SecurablePrincipal organization ) {
        var principaleTitle = organization.getName() + " - ADMIN";
        var principalId = organization.getId().toString() + "|" + principaleTitle;
        var rolePrincipal = new Principal( PrincipalType.ROLE, principalId );
        return new Role( Optional.empty(),
                organization.getId(),
                rolePrincipal,
                principaleTitle,
                Optional.of( "Administrators of this organization" ) );
    }

    private static Predicate getOrganizationPredicate( UUID organizationId ) {
        return Predicates.and(
                Predicates.equal( "principalType", PrincipalType.ORGANIZATION ),
                Predicates.equal( "aclKey[0]", organizationId ) );
    }
}
