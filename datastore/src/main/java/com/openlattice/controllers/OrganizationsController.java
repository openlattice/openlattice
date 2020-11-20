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

package com.openlattice.controllers;

import com.auth0.json.mgmt.users.User;
import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.openlattice.apps.services.AppService;
import com.openlattice.assembler.Assembler;
import com.openlattice.authorization.*;
import com.openlattice.authorization.securable.SecurableObjectType;
import com.openlattice.authorization.util.AuthorizationUtilsKt;
import com.openlattice.controllers.exceptions.ForbiddenException;
import com.openlattice.datastore.services.EdmManager;
import com.openlattice.datastore.services.EntitySetManager;
import com.openlattice.organization.OrganizationEntitySetFlag;
import com.openlattice.organization.OrganizationIntegrationAccount;
import com.openlattice.organization.OrganizationMember;
import com.openlattice.organization.OrganizationPrincipal;
import com.openlattice.organization.OrganizationsApi;
import com.openlattice.organization.roles.Role;
import com.openlattice.organizations.ExternalDatabaseManagementService;
import com.openlattice.organizations.Grant;
import com.openlattice.organizations.HazelcastOrganizationService;
import com.openlattice.organizations.Organization;
import com.openlattice.organizations.OrganizationMetadataEntitySetIds;
import com.openlattice.organizations.OrganizationMetadataEntitySetsService;
import com.openlattice.organizations.roles.SecurePrincipalsManager;
import org.apache.commons.lang3.NotImplementedException;
import org.jetbrains.annotations.NotNull;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import javax.inject.Inject;
import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.google.common.base.Preconditions.checkArgument;

@RestController
@RequestMapping( OrganizationsApi.CONTROLLER )
public class OrganizationsController implements AuthorizingComponent, OrganizationsApi {

    @Inject
    private AuthorizationManager authorizations;

    @Inject
    private HazelcastOrganizationService organizations;

    @Inject
    private Assembler assembler;

    @Inject
    private SecurableObjectResolveTypeService securableObjectTypes;

    @Inject
    private SecurePrincipalsManager principalService;

    @Inject
    private EntitySetManager entitySetManager;

    @Inject
    private ExternalDatabaseManagementService edms;

    @Inject
    private AppService appService;

    @Inject
    private OrganizationMetadataEntitySetsService organizationMetadataEntitySetsService;

    @Inject
    private ExternalDatabaseManagementService externalDatabaseManagementService;

    @Inject
    private EdmManager edmService;

    @Timed
    @Override
    @GetMapping(
            value = { "", "/" },
            produces = MediaType.APPLICATION_JSON_VALUE )
    public Iterable<Organization> getOrganizations() {

        Set<AclKey> authorizedRoles = getAccessibleObjects( SecurableObjectType.Role, EnumSet.of( Permission.READ ) )
                .filter( Objects::nonNull ).collect( Collectors.toSet() );

        Iterable<Organization> orgs = organizations.getOrganizations(
                getAccessibleObjects( SecurableObjectType.Organization, EnumSet.of( Permission.READ ) )
                        .parallel()
                        .filter( Objects::nonNull )
                        .map( AuthorizationUtilsKt::getLastAclKeySafely ),
                true
        );

        return StreamSupport.stream( orgs.spliterator(), false ).peek( org ->
                org.getRoles().removeIf( role -> !authorizedRoles.contains( role.getAclKey() ) )
        ).collect( Collectors.toList() );
    }

    @Timed
    @Override
    @GetMapping(
            value = { METADATA },
            produces = MediaType.APPLICATION_JSON_VALUE )
    public Iterable<OrganizationPrincipal> getMetadataOfOrganizations() {

        Set<UUID> authorizedOrganizationIds = getAccessibleObjects( SecurableObjectType.Organization,
                EnumSet.of( Permission.READ ) ).map( ak -> ak.get( 0 ) ).collect( Collectors.toSet() );

        return organizations.getOrganizationPrincipals( authorizedOrganizationIds );
    }

    @Timed
    @Override
    @PostMapping(
            value = { "", "/" },
            consumes = MediaType.APPLICATION_JSON_VALUE )
    public UUID createOrganizationIfNotExists( @RequestBody Organization organization ) {
        checkArgument( organization.getConnections().isEmpty() || isAdmin(),
                "Must be admin to specify auto-enrollments" );
        organizations.createOrganization( Principals.getCurrentUser(), organization );
        securableObjectTypes.createSecurableObjectType( new AclKey( organization.getId() ),
                SecurableObjectType.Organization );
        return organization.getId();
    }

    @Timed
    @Override
    @GetMapping(
            value = ID_PATH,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public Organization getOrganization( @PathVariable( ID ) UUID organizationId ) {
        ensureRead( organizationId );
        //TODO: Re-visit roles within an organization being defined as roles which have read on that organization.
        Organization org = organizations.getOrganization( organizationId );
        if ( org != null ) {
            Set<AclKey> authorizedRoleAclKeys = getAuthorizedRoleAclKeys( org.getRoles() );
            org.getRoles().removeIf( role -> !authorizedRoleAclKeys.contains( role.getAclKey() ) );
        }
        return org;
    }

    @Timed
    @Override
    @DeleteMapping( value = ID_PATH, produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Void destroyOrganization( @PathVariable( ID ) UUID organizationId ) {
        AclKey aclKey = ensureOwner( organizationId );

        ensureObjectCanBeDeleted( organizationId );
        organizations.ensureOrganizationExists( organizationId );

        organizations.destroyOrganization( organizationId );
        edms.deleteOrganizationExternalDatabase( organizationId );
        authorizations.deletePermissions( aclKey );
        securableObjectTypes.deleteSecurableObjectType( new AclKey( organizationId ) );
        return null;
    }

    @Override
    @GetMapping( value = ID_PATH + SET_ID_PATH + TRANSPORT, produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Void transportEntitySet(
            @PathVariable( ID ) UUID organizationId,
            @PathVariable( SET_ID ) UUID entitySetId ) {
        organizations.ensureOrganizationExists( organizationId );
        ensureRead( organizationId );
        ensureTransportAccess( new AclKey( entitySetId ) );
        edms.transportEntitySet( organizationId, entitySetId );
        return null;
    }

    @Override
    @GetMapping( value = ID_PATH + SET_ID_PATH + DESTROY, produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Void destroyTransportedEntitySet(
            @PathVariable( ID ) UUID organizationId,
            @PathVariable( SET_ID ) UUID entitySetId ) {
        organizations.ensureOrganizationExists( organizationId );
        ensureRead( organizationId );
        ensureTransportAccess( new AclKey( entitySetId ) );
        edms.destroyTransportedEntitySet( entitySetId );
        return null;
    }

    @Timed
    @Override
    @GetMapping( value = ID_PATH + INTEGRATION, produces = MediaType.APPLICATION_JSON_VALUE )
    public OrganizationIntegrationAccount getOrganizationIntegrationAccount( @PathVariable( ID ) UUID organizationId ) {
        ensureOwner( organizationId );
        return assembler.getOrganizationIntegrationAccount( organizationId );
    }

    @Timed
    @Override
    @PatchMapping( value = ID_PATH + INTEGRATION, produces = MediaType.APPLICATION_JSON_VALUE )
    public OrganizationIntegrationAccount rollOrganizationIntegrationAccount(
            @PathVariable( ID ) UUID organizationId ) {
        ensureOwner( organizationId );
        var account = assembler.rollIntegrationAccount( organizationId, PrincipalType.ORGANIZATION );
        return new OrganizationIntegrationAccount( account.getUsername(), account.getCredential() );
    }

    @Timed
    @Override
    @GetMapping( value = ID_PATH + ENTITY_SETS, produces = MediaType.APPLICATION_JSON_VALUE )
    public Map<UUID, Set<OrganizationEntitySetFlag>> getOrganizationEntitySets(
            @PathVariable( ID ) UUID organizationId ) {
        ensureRead( organizationId );
        return getOrganizationEntitySets( organizationId, EnumSet.allOf( OrganizationEntitySetFlag.class ) );
    }

    @Timed
    @Override
    @PostMapping( value = ID_PATH + ENTITY_SETS, produces = MediaType.APPLICATION_JSON_VALUE )
    public Map<UUID, Set<OrganizationEntitySetFlag>> getOrganizationEntitySets(
            @PathVariable( ID ) UUID organizationId,
            @RequestBody EnumSet<OrganizationEntitySetFlag> flagFilter ) {
        ensureRead( organizationId );
        var orgPrincipal = organizations.getOrganizationPrincipal( organizationId );
        if ( orgPrincipal == null ) {
            return null;
        }
        var internal = entitySetManager.getEntitySetsForOrganization( organizationId );
        var external = authorizations.getAuthorizedObjectsOfType(
                orgPrincipal.getPrincipal(),
                SecurableObjectType.EntitySet,
                EnumSet.of( Permission.MATERIALIZE ) );
        var materialized = assembler.getMaterializedEntitySetsInOrganization( organizationId );

        Map<UUID, Set<OrganizationEntitySetFlag>> entitySets = new HashMap<>( 2 * internal.size() );

        if ( flagFilter.contains( OrganizationEntitySetFlag.INTERNAL ) ) {
            internal.forEach( entitySetId -> entitySets
                    .merge( entitySetId, EnumSet.of( OrganizationEntitySetFlag.INTERNAL ), ( lhs, rhs ) -> {
                        lhs.addAll( rhs );
                        return lhs;
                    } ) );

        }
        if ( flagFilter.contains( OrganizationEntitySetFlag.EXTERNAL ) ) {
            external.map( aclKey -> aclKey.get( 0 ) ).forEach( entitySetId -> entitySets
                    .merge( entitySetId, EnumSet.of( OrganizationEntitySetFlag.EXTERNAL ), ( lhs, rhs ) -> {
                        lhs.addAll( rhs );
                        return lhs;
                    } ) );

        }

        if ( flagFilter.contains( OrganizationEntitySetFlag.MATERIALIZED )
                || flagFilter.contains( OrganizationEntitySetFlag.EDM_UNSYNCHRONIZED )
                || flagFilter.contains( OrganizationEntitySetFlag.DATA_UNSYNCHRONIZED ) ) {
            materialized.forEach( ( entitySetId, flags ) -> {
                if ( flagFilter.contains( OrganizationEntitySetFlag.MATERIALIZED ) ) {
                    entitySets.merge( entitySetId, EnumSet.of( OrganizationEntitySetFlag.MATERIALIZED ),
                            ( lhs, rhs ) -> {
                                lhs.addAll( rhs );
                                return lhs;
                            } );
                }

                if ( flagFilter.contains( OrganizationEntitySetFlag.EDM_UNSYNCHRONIZED )
                        && flags.contains( OrganizationEntitySetFlag.EDM_UNSYNCHRONIZED ) ) {
                    entitySets.merge( entitySetId, EnumSet.of( OrganizationEntitySetFlag.EDM_UNSYNCHRONIZED ),
                            ( lhs, rhs ) -> {
                                lhs.addAll( rhs );
                                return lhs;
                            } );
                }

                if ( flagFilter.contains( OrganizationEntitySetFlag.DATA_UNSYNCHRONIZED )
                        && flags.contains( OrganizationEntitySetFlag.DATA_UNSYNCHRONIZED ) ) {
                    entitySets.merge( entitySetId, EnumSet.of( OrganizationEntitySetFlag.DATA_UNSYNCHRONIZED ),
                            ( lhs, rhs ) -> {
                                lhs.addAll( rhs );
                                return lhs;
                            } );
                }
            } );
        }

        return entitySets;
    }

    @Timed
    @Override
    @PostMapping(
            value = ID_PATH + ENTITY_SETS + ASSEMBLE,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public Map<UUID, Set<OrganizationEntitySetFlag>> assembleEntitySets(
            @PathVariable( ID ) UUID organizationId,
            @RequestBody Map<UUID, Integer> refreshRatesOfEntitySets ) {
        throw new NotImplementedException( "DBT will fill this in." );
    }

    @Timed
    @Override
    @PostMapping( ID_PATH + SET_ID_PATH + SYNCHRONIZE )
    public Void synchronizeEdmChanges(
            @PathVariable( ID ) UUID organizationId,
            @PathVariable( SET_ID ) UUID entitySetId ) {
        throw new NotImplementedException( "DBT will fill this in." );
    }

    @Timed
    @Override
    @PostMapping( ID_PATH + SET_ID_PATH + REFRESH )
    public Void refreshDataChanges(
            @PathVariable( ID ) UUID organizationId,
            @PathVariable( SET_ID ) UUID entitySetId ) {
        // the person requesting refresh should be the owner of the organization
        ensureOwner( organizationId );

        throw new NotImplementedException( "DBT will fill this in." );
    }

    @Override
    @PutMapping( ID_PATH + SET_ID_PATH + REFRESH_RATE )
    public Void updateRefreshRate(
            @PathVariable( ID ) UUID organizationId,
            @PathVariable( SET_ID ) UUID entitySetId,
            @RequestBody Integer refreshRate ) {
        ensureOwner( organizationId );

        var refreshRateInMilliSecs = getRefreshRateMillisFromMins( refreshRate );

        assembler.updateRefreshRate( organizationId, entitySetId, refreshRateInMilliSecs );
        return null;
    }

    @Override
    @DeleteMapping( ID_PATH + SET_ID_PATH + REFRESH_RATE )
    public Void deleteRefreshRate( @PathVariable( ID ) UUID organizationId, @PathVariable( SET_ID ) UUID entitySetId ) {
        ensureOwner( organizationId );

        assembler.updateRefreshRate( organizationId, entitySetId, null );
        return null;
    }

    private Long getRefreshRateMillisFromMins( Integer refreshRateInMins ) {
        if ( refreshRateInMins < 1 ) {
            throw new IllegalArgumentException( "Minimum refresh rate is 1 minute." );
        }

        // convert mins to millisecs
        return refreshRateInMins.longValue() * 3600L;
    }

    @Timed
    @Override
    @PutMapping(
            value = ID_PATH + TITLE,
            consumes = MediaType.TEXT_PLAIN_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Void updateTitle( @PathVariable( ID ) UUID organizationId, @RequestBody String title ) {
        ensureOwner( organizationId );
        organizations.updateTitle( organizationId, title );
        return null;
    }

    @Timed
    @Override
    @PutMapping(
            value = ID_PATH + DESCRIPTION,
            consumes = MediaType.TEXT_PLAIN_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Void updateDescription( @PathVariable( ID ) UUID organizationId, @RequestBody String description ) {
        ensureOwner( organizationId );
        organizations.updateDescription( organizationId, description );
        return null;
    }

    @Timed
    @Override
    @GetMapping(
            value = ID_PATH + EMAIL_DOMAINS,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public Set<String> getAutoApprovedEmailDomains( @PathVariable( ID ) UUID organizationId ) {
        ensureOwner( organizationId );
        return organizations.getEmailDomains( organizationId );
    }

    @Timed
    @Override
    @PutMapping(
            value = ID_PATH + EMAIL_DOMAINS,
            consumes = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Void setAutoApprovedEmailDomain(
            @PathVariable( ID ) UUID organizationId,
            @RequestBody Set<String> emailDomains ) {
        ensureAdminAccess();
        organizations.setEmailDomains( organizationId, emailDomains );
        return null;
    }

    @Timed
    @Override
    @PostMapping(
            value = ID_PATH + EMAIL_DOMAINS,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public Void addEmailDomains(
            @PathVariable( ID ) UUID organizationId,
            @RequestBody Set<String> emailDomains ) {
        ensureAdminAccess();
        organizations.addEmailDomains( organizationId, emailDomains );
        return null;
    }

    @Timed
    @Override
    @DeleteMapping(
            value = ID_PATH + EMAIL_DOMAINS,
            consumes = MediaType.APPLICATION_JSON_VALUE )
    public Void removeEmailDomains(
            @PathVariable( ID ) UUID organizationId,
            @RequestBody Set<String> emailDomains ) {
        ensureAdminAccess();
        organizations.removeEmailDomains( organizationId, emailDomains );
        return null;
    }

    @Timed
    @Override
    @PutMapping(
            value = ID_PATH + EMAIL_DOMAINS + EMAIL_DOMAIN_PATH )
    public Void addEmailDomain(
            @PathVariable( ID ) UUID organizationId,
            @PathVariable( EMAIL_DOMAIN ) String emailDomain ) {
        ensureAdminAccess();
        organizations.addEmailDomains( organizationId, ImmutableSet.of( emailDomain ) );
        return null;
    }

    @Timed
    @Override
    @DeleteMapping(
            value = ID_PATH + EMAIL_DOMAINS + EMAIL_DOMAIN_PATH )
    public Void removeEmailDomain(
            @PathVariable( ID ) UUID organizationId,
            @PathVariable( EMAIL_DOMAIN ) String emailDomain ) {
        ensureAdminAccess();
        organizations.removeEmailDomains( organizationId, ImmutableSet.of( emailDomain ) );
        return null;
    }

    @Timed
    @Override
    @GetMapping(
            value = PRINCIPALS + MEMBERS + COUNT,
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE
    )
    public Map<UUID, Integer> getMemberCountForOrganizations( Set<UUID> organizationIds ) {
        EnumSet<Permission> readPermissions = EnumSet.of( Permission.READ );
        accessCheck( organizationIds.stream().collect( Collectors.toMap( AclKey::new, id -> readPermissions ) ) );

        return organizations.getMemberCountsForOrganizations( organizationIds );
    }

    @Timed
    @Override
    @GetMapping(
            value = ID_PATH + PRINCIPALS + MEMBERS )
    public Iterable<OrganizationMember> getMembers( @PathVariable( ID ) UUID organizationId ) {
        ensureRead( organizationId );
        Set<Principal> members = organizations.getMembers( organizationId );
        Collection<SecurablePrincipal> securablePrincipals = principalService.getSecurablePrincipals( members );
        return securablePrincipals
                .stream()
                .map( sp -> new OrganizationMember( sp,
                        principalService.getUser( sp.getName() ),
                        principalService.getAllPrincipals( sp ) ) )::iterator;

    }

    @Timed
    @Override
    @PutMapping(
            value = ID_PATH + PRINCIPALS + MEMBERS + USER_ID_PATH )
    public Void addMember(
            @PathVariable( ID ) UUID organizationId,
            @PathVariable( USER_ID ) String userId ) {
        ensureOwnerAccess( new AclKey( organizationId ) );
        organizations.addMembers( organizationId, ImmutableSet.of( new Principal( PrincipalType.USER, userId ) ) );
        return null;
    }

    @Timed
    @Override
    @DeleteMapping(
            value = ID_PATH + PRINCIPALS + MEMBERS + USER_ID_PATH )
    public Void removeMember(
            @PathVariable( ID ) UUID organizationId,
            @PathVariable( USER_ID ) String userId ) {
        ensureOwnerAccess( new AclKey( organizationId ) );
        organizations.removeMembers( organizationId, ImmutableSet.of( new Principal( PrincipalType.USER, userId ) ) );
        edms.revokeAllPrivilegesFromMember( organizationId, userId );
        return null;
    }

    @Timed
    @Override
    @PostMapping(
            value = ROLES,
            consumes = MediaType.APPLICATION_JSON_VALUE )
    public UUID createRole( @RequestBody Role role ) {
        ensureOwner( role.getOrganizationId() );
        //We only create the role, but do not necessarily assign it to ourselves.
        organizations.createRoleIfNotExists( Principals.getCurrentUser(), role );
        return role.getId();
    }

    @Timed
    @Override
    @GetMapping(
            value = ID_PATH + PRINCIPALS + ROLES,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public Set<Role> getRoles( @PathVariable( ID ) UUID organizationId ) {
        ensureRead( organizationId );
        Set<Role> roles = organizations.getRoles( organizationId );
        Set<AclKey> authorizedRoleAclKeys = getAuthorizedRoleAclKeys( roles );
        return Sets.filter( roles, role -> role != null && authorizedRoleAclKeys.contains( role.getAclKey() ) );
    }

    private Set<AclKey> getAuthorizedRoleAclKeys( Set<Role> roles ) {
        return authorizations
                .accessChecksForPrincipals( roles.stream()
                        .map( role -> new AccessCheck( role.getAclKey(), EnumSet.of( Permission.READ ) ) )
                        .collect( Collectors.toSet() ), Principals.getCurrentPrincipals() )
                .filter( authorization -> authorization.getPermissions().get( Permission.READ ) )
                .map( Authorization::getAclKey ).collect( Collectors.toSet() );
    }

    @Timed
    @Override
    @GetMapping(
            value = ID_PATH + PRINCIPALS + ROLES + ROLE_ID_PATH,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public Role getRole( @PathVariable( ID ) UUID organizationId, @PathVariable( ROLE_ID ) UUID roleId ) {
        AclKey aclKey = new AclKey( organizationId, roleId );
        if ( isAuthorized( Permission.READ ).test( aclKey ) ) {
            return principalService.getRole( organizationId, roleId );
        } else {
            throw new ForbiddenException( "Unable to find role: " + aclKey );
        }
    }

    @Timed
    @Override
    @PutMapping(
            value = ID_PATH + PRINCIPALS + ROLES + ROLE_ID_PATH + TITLE,
            consumes = MediaType.TEXT_PLAIN_VALUE )
    public Void updateRoleTitle(
            @PathVariable( ID ) UUID organizationId,
            @PathVariable( ROLE_ID ) UUID roleId,
            @RequestBody String title ) {
        ensureRoleAdminAccess( organizationId, roleId );
        //TODO: Do this in a less crappy way
        principalService.updateTitle( new AclKey( organizationId, roleId ), title );
        return null;
    }

    @Timed
    @Override
    @PutMapping(
            value = ID_PATH + PRINCIPALS + ROLES + ROLE_ID_PATH + DESCRIPTION,
            consumes = MediaType.TEXT_PLAIN_VALUE )
    public Void updateRoleDescription(
            @PathVariable( ID ) UUID organizationId,
            @PathVariable( ROLE_ID ) UUID roleId,
            @RequestBody String description ) {
        ensureRoleAdminAccess( organizationId, roleId );
        principalService.updateDescription( new AclKey( organizationId, roleId ), description );
        return null;
    }

    @Timed
    @Override
    @PutMapping(
            value = ID_PATH + PRINCIPALS + ROLES + ROLE_ID_PATH + GRANT,
            consumes = MediaType.APPLICATION_JSON_VALUE
    )
    public Void updateRoleGrant(
            @PathVariable( ID ) UUID organizationId,
            @PathVariable( ROLE_ID ) UUID roleId,
            @NotNull @RequestBody Grant grant ) {
        ensureRoleAdminAccess( organizationId, roleId );
        organizations.updateRoleGrant( organizationId, roleId, grant );
        return null;
    }

    @Timed
    @Override
    @DeleteMapping(
            value = ID_PATH + PRINCIPALS + ROLES + ROLE_ID_PATH )
    public Void deleteRole( @PathVariable( ID ) UUID organizationId, @PathVariable( ROLE_ID ) UUID roleId ) {
        ensureRoleAdminAccess( organizationId, roleId );
        ensureObjectCanBeDeleted( roleId );
        ensureRoleIsNotAdminRole( organizationId, roleId );
        ensureRoleNotUsedByApp( organizationId, roleId );
        principalService.deletePrincipal( new AclKey( organizationId, roleId ) );
        return null;
    }

    @Timed
    @Override
    @GetMapping(
            value = ID_PATH + PRINCIPALS + ROLES + ROLE_ID_PATH + MEMBERS,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public Iterable<User> getAllUsersOfRole(
            @PathVariable( ID ) UUID organizationId,
            @PathVariable( ROLE_ID ) UUID roleId ) {
        ensureRead( organizationId );
        return principalService.getAllUserProfilesWithPrincipal( new AclKey( organizationId, roleId ) );
    }

    @Timed
    @Override
    @PutMapping(
            value = ID_PATH + PRINCIPALS + ROLES + ROLE_ID_PATH + MEMBERS + USER_ID_PATH )
    public Void addRoleToUser(
            @PathVariable( ID ) UUID organizationId,
            @PathVariable( ROLE_ID ) UUID roleId,
            @PathVariable( USER_ID ) String userId ) {
        ensureOwnerAccess( new AclKey( organizationId, roleId ) );

        organizations.addRoleToPrincipalInOrganization( organizationId, roleId,
                new Principal( PrincipalType.USER, userId ) );
        return null;
    }

    @Timed
    @Override
    @DeleteMapping(
            value = ID_PATH + PRINCIPALS + ROLES + ROLE_ID_PATH + MEMBERS + USER_ID_PATH )
    public Void removeRoleFromUser(
            @PathVariable( ID ) UUID organizationId,
            @PathVariable( ROLE_ID ) UUID roleId,
            @PathVariable( USER_ID ) String userId ) {
        ensureOwnerAccess( new AclKey( organizationId, roleId ) );

        organizations.removeRoleFromUser( new AclKey( organizationId, roleId ),
                new Principal( PrincipalType.USER, userId ) );
        return null;
    }

    @Override
    @Timed
    @PostMapping( value = ID_PATH + CONNECTIONS, consumes = MediaType.APPLICATION_JSON_VALUE )
    public Void addConnections( @PathVariable( ID ) UUID organizationId, @RequestBody Set<String> connections ) {
        ensureAdminAccess();
        organizations.addConnections( organizationId, connections );
        return null;
    }

    @Timed
    @PutMapping( value = ID_PATH + CONNECTIONS, consumes = MediaType.APPLICATION_JSON_VALUE )
    @Override
    public Void setConnections( @PathVariable( ID ) UUID organizationId, @RequestBody Set<String> connections ) {
        ensureAdminAccess();
        organizations.setConnections( organizationId, connections );
        return null;
    }

    @Override
    @Timed
    @PutMapping( value = ID_PATH + METADATA_ENTITY_SET_IDS, consumes = MediaType.APPLICATION_JSON_VALUE )
    public Void setMetadataEntitySetIds(
            @PathVariable( ID ) UUID organizationId,
            @RequestBody OrganizationMetadataEntitySetIds entitySetIds ) {
        ensureOwner( organizationId );
        organizations.setOrganizationMetadataEntitySetIds( organizationId, entitySetIds );
        return null;
    }

    @Override
    @Timed
    @PostMapping( value = ID_PATH + METADATA, consumes = MediaType.APPLICATION_JSON_VALUE )
    public Void importMetadata( @PathVariable( ID ) UUID organizationId ) {
        ensureAdminAccess();
        ensureOwner( organizationId );
        AclKey adminRoleAclKey = organizations.getAdminRoleAclKey( organizationId );
        organizationMetadataEntitySetsService.initializeOrganizationMetadataEntitySets( principalService
                .getRole( adminRoleAclKey.get( 0 ), adminRoleAclKey.get( 1 ) ) );

        var tables = externalDatabaseManagementService
                .getExternalDatabaseTablesWithColumns( organizationId );
        tables
                .forEach( ( t, c ) -> {
                    organizationMetadataEntitySetsService.addDataset( organizationId, t.getSecond() );
                    organizationMetadataEntitySetsService
                            .addDatasetColumns(
                                    organizationId,
                                    t.getSecond(),
                                    c.stream().map( Entry::getValue ).collect( Collectors.toList() ) );
                } );

        entitySetManager
                .getEntitySetsForOrganization( organizationId )
                .forEach( e -> {
                    var entitySet = Objects.requireNonNull( entitySetManager.getEntitySet( e ) );
                    var propertyTypes = edmService.getPropertyTypesOfEntityType( entitySet.getEntityTypeId() );

                    organizationMetadataEntitySetsService.addDataset( entitySet );
                    organizationMetadataEntitySetsService.addDatasetColumns( entitySet, propertyTypes.values() );
                } );

        return null;
    }

    @Timed
    @DeleteMapping( value = ID_PATH + CONNECTIONS, consumes = MediaType.APPLICATION_JSON_VALUE )
    @Override
    public Void removeConnections( @PathVariable( ID ) UUID organizationId, @RequestBody Set<String> connections ) {
        organizations.removeConnections( organizationId, connections );
        return null;
    }

    @Timed
    @PostMapping( value = PROMOTE + ID_PATH, consumes = MediaType.TEXT_PLAIN_VALUE )
    @Override
    public Void promoteStagingTable( @PathVariable( ID ) UUID organizationId, @RequestBody String tableName ) {
        ensureOwner( organizationId );
        edms.promoteStagingTable( organizationId, tableName );
        return null;
    }

    @Override
    @GetMapping( value = ID_PATH + DATABASE, produces = MediaType.APPLICATION_JSON_VALUE )
    public String getOrganizationDatabaseName( @PathVariable( ID ) UUID organizationId ) {
        ensureRead( organizationId );
        return organizations.getOrganizationDatabaseName( organizationId );
    }

    @Override
    @PatchMapping( value = ID_PATH + DATABASE, consumes = { MediaType.TEXT_PLAIN_VALUE,
            MediaType.APPLICATION_FORM_URLENCODED_VALUE } )
    public Void renameOrganizationDatabase(
            @PathVariable( ID ) UUID organizationId,
            @RequestBody String newDatabaseName ) {
        ensureOwner( organizationId );
        organizations.renameOrganizationDatabase( organizationId, newDatabaseName );
        return null;
    }

    private void ensureRoleAdminAccess( UUID organizationId, UUID roleId ) {
        ensureOwner( organizationId );

        AclKey aclKey = new AclKey( organizationId, roleId );
        accessCheck( aclKey, EnumSet.of( Permission.OWNER ) );
    }

    @Override
    public AuthorizationManager getAuthorizationManager() {
        return authorizations;
    }

    private AclKey ensureOwner( UUID organizationId ) {
        AclKey aclKey = new AclKey( organizationId );
        accessCheck( aclKey, EnumSet.of( Permission.OWNER ) );
        return aclKey;
    }

    private void ensureRead( UUID organizationId ) {
        ensureReadAccess( new AclKey( organizationId ) );
    }

    private void ensureRoleNotUsedByApp( UUID organizationId, UUID roleId ) {
        AclKey aclKey = new AclKey( organizationId, roleId );
        appService.getOrganizationAppsByAppId( organizationId ).forEach( ( appId, appTypeSetting ) -> {
            appTypeSetting.getRoles().forEach( ( appRoleId, roleAclKey ) -> {
                if ( roleAclKey.equals( aclKey ) ) {
                    throw new IllegalArgumentException( "Role " + aclKey.toString()
                            + " cannot be deleted because it is tied to installation of app " + appId.toString() );
                }
            } );
        } );

    }

    private void ensureRoleIsNotAdminRole( UUID organizationId, UUID roleId ) {
        AclKey adminRoleAclKey = organizations.getAdminRoleAclKey( organizationId );
        Preconditions.checkArgument( !roleId.equals( adminRoleAclKey.get( 1 ) ),
                "Role " + adminRoleAclKey.toString()
                        + " cannot be deleted because it is the organization's admin role." );
    }

}
