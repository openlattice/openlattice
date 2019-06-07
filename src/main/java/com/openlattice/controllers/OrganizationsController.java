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

import com.codahale.metrics.annotation.Timed;
import com.dataloom.streams.StreamUtil;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.openlattice.assembler.Assembler;
import com.openlattice.authorization.*;
import com.openlattice.authorization.securable.SecurableObjectType;
import com.openlattice.authorization.util.AuthorizationUtils;
import com.openlattice.controllers.exceptions.ForbiddenException;
import com.openlattice.controllers.exceptions.ResourceNotFoundException;
import com.openlattice.datastore.services.EdmManager;
import com.openlattice.directory.pojo.Auth0UserBasic;
import com.openlattice.edm.type.PropertyType;
import com.openlattice.organization.*;
import com.openlattice.organization.roles.Role;
import com.openlattice.organizations.HazelcastOrganizationService;
import com.openlattice.organizations.roles.SecurePrincipalsManager;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import javax.inject.Inject;
import java.util.*;
import java.util.stream.Collectors;

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
    private EdmManager edm;

    @Inject
    private EdmAuthorizationHelper authzHelper;

    @Timed
    @Override
    @GetMapping(
            value = { "", "/" },
            produces = MediaType.APPLICATION_JSON_VALUE )
    public Iterable<Organization> getOrganizations() {
        return organizations.getOrganizations(
                getAccessibleObjects( SecurableObjectType.Organization, EnumSet.of( Permission.READ ) )
                        .parallel()
                        .filter( Predicates.notNull()::apply )
                        .map( AuthorizationUtils::getLastAclKeySafely )
        );
    }

    @Timed
    @Override
    @PostMapping(
            value = { "", "/" },
            consumes = MediaType.APPLICATION_JSON_VALUE )
    public UUID createOrganizationIfNotExists( @RequestBody Organization organization ) {
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
        Set<Role> authorizedRoles = getAuthorizedRoles( organizationId, Permission.READ );
        return new Organization(
                org.getSecurablePrincipal(),
                org.getAutoApprovedEmails(),
                org.getMembers(),
                authorizedRoles,
                org.getApps(),
                org.getPhoneNumber());
    }

    @Timed
    @Override
    @DeleteMapping( value = ID_PATH, produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Void destroyOrganization( @PathVariable( ID ) UUID organizationId ) {
        AclKey aclKey = ensureOwner( organizationId );

        organizations.destroyOrganization( organizationId );
        authorizations.deletePermissions( aclKey );
        securableObjectTypes.deleteSecurableObjectType( new AclKey( organizationId ) );
        return null;
    }

    @Timed
    @Override
    @PostMapping(
            value = OrganizationsApi.SET_PHONE_PATH,
            consumes = MediaType.APPLICATION_JSON_VALUE )
    public Void setOrganizationPhoneNumber( UUID organizationId, String phoneNumber ) {
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
        final var orgPrincipal = organizations.getOrganizationPrincipal( organizationId );
        final var internal = edm.getEntitySetsForOrganization( organizationId );
        final var external = authorizations.getAuthorizedObjectsOfType(
                orgPrincipal.getPrincipal(),
                SecurableObjectType.EntitySet,
                EnumSet.of( Permission.MATERIALIZE ) );
        final var materialized = assembler.getMaterializedEntitySetsInOrganization( organizationId );

        final Map<UUID, Set<OrganizationEntitySetFlag>> entitySets = new HashMap<>( 2 * internal.size() );

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
            @RequestBody Set<UUID> entitySetIds ) {
        final var authorizedPropertyTypesByEntitySet =
                getAuthorizedPropertiesForMaterialization( organizationId, entitySetIds );
        return assembler.materializeEntitySets( organizationId, authorizedPropertyTypesByEntitySet );
    }

    @Timed
    @Override
    @PostMapping( ID_PATH + SET_ID_PATH + SYNCHRONIZE )
    public Void synchronizeEdmChanges(
            @PathVariable( ID ) UUID organizationId,
            @PathVariable( SET_ID ) UUID entitySetId ) {
        // we basically re-materialize in this case
        final var authorizedPropertyTypesByEntitySet =
                getAuthorizedPropertiesForMaterialization( organizationId, Set.of( entitySetId ) );

        assembler.synchronizeMaterializedEntitySet(
                organizationId,
                entitySetId,
                authorizedPropertyTypesByEntitySet.get( entitySetId ) );
        return null;
    }

    @Timed
    @Override
    @PostMapping( ID_PATH + SET_ID_PATH + REFRESH )
    public Void refreshDataChanges(
            @PathVariable( ID ) UUID organizationId,
            @PathVariable( SET_ID ) UUID entitySetId ) {
        // we need authorized property types to re-build materialized view, since the only way to refresh it is
        // to drop and re-import cascade to organization database
        final var authorizedPropertyTypesByEntitySet =
                getAuthorizedPropertiesForMaterialization( organizationId, Set.of( entitySetId ) );

        assembler.refreshMaterializedEntitySet(
                organizationId,
                entitySetId,
                authorizedPropertyTypesByEntitySet.get( entitySetId ) );
        return null;
    }

    private Map<UUID, Map<UUID, PropertyType>> getAuthorizedPropertiesForMaterialization(
            UUID organizationId,
            Set<UUID> entitySetIds ) {
        // materialize should be a property level permission that can only be granted to organization principals and
        // the person requesting materialize should be the owner of the organization
        ensureOwner( organizationId );
        final var organizationPrincipal = organizations.getOrganizationPrincipal( organizationId );

        if ( organizationPrincipal == null ) {
            //This will be rare, since it is unlikely you have access to an organization that does not exist.
            throw new ResourceNotFoundException( "Organization does not exist." );
        }

        // check materialization on all linking and normal entity sets
        final var entitySets = edm.getEntitySetsAsMap( entitySetIds );
        final var allEntitySetIds = entitySets.values().stream()
                .flatMap( entitySet -> {
                    var entitySetIdsToCheck = Sets.newHashSet( entitySet.getId() );
                    if ( entitySet.isLinking() ) {
                        entitySetIdsToCheck.addAll( entitySet.getLinkedEntitySets() );
                    }

                    return entitySetIdsToCheck.stream();
                } ).collect( Collectors.toList() );

        allEntitySetIds.forEach( entitySetId -> ensureMaterialize( entitySetId, organizationPrincipal ) );

        // first we collect authorized property types of normal entity sets and then for each linking entity set, we
        // check materialization on normal entity sets and get the intersection of their authorized property types
        return authzHelper.getAuthorizedPropertiesOnEntitySets(
                entitySetIds, EnumSet.of( Permission.MATERIALIZE ), Set.of( organizationPrincipal.getPrincipal() ) );
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
        return organizations.getAutoApprovedEmailDomains( organizationId );
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
        ensureOwner( organizationId );
        organizations.setAutoApprovedEmailDomains( organizationId, emailDomains );
        return null;
    }

    @Timed
    @Override
    @PostMapping(
            value = ID_PATH + EMAIL_DOMAINS,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public Void addAutoApprovedEmailDomains(
            @PathVariable( ID ) UUID organizationId,
            @RequestBody Set<String> emailDomains ) {
        ensureOwner( organizationId );
        organizations.addAutoApprovedEmailDomains( organizationId, emailDomains );
        return null;
    }

    @Timed
    @Override
    @DeleteMapping(
            value = ID_PATH + EMAIL_DOMAINS,
            consumes = MediaType.APPLICATION_JSON_VALUE )
    public Void removeAutoApprovedEmailDomains(
            @PathVariable( ID ) UUID organizationId,
            @RequestBody Set<String> emailDomains ) {
        ensureOwner( organizationId );
        organizations.removeAutoApprovedEmailDomains( organizationId, emailDomains );
        return null;
    }

    @Timed
    @Override
    @PutMapping(
            value = ID_PATH + EMAIL_DOMAINS + EMAIL_DOMAIN_PATH )
    public Void addAutoApprovedEmailDomain(
            @PathVariable( ID ) UUID organizationId,
            @PathVariable( EMAIL_DOMAIN ) String emailDomain ) {
        ensureOwner( organizationId );
        organizations.addAutoApprovedEmailDomains( organizationId, ImmutableSet.of( emailDomain ) );
        return null;
    }

    @Timed
    @Override
    @DeleteMapping(
            value = ID_PATH + EMAIL_DOMAINS + EMAIL_DOMAIN_PATH )
    public Void removeAutoApprovedEmailDomain(
            @PathVariable( ID ) UUID organizationId,
            @PathVariable( EMAIL_DOMAIN ) String emailDomain ) {
        ensureOwner( organizationId );
        organizations.removeAutoApprovedEmailDomains( organizationId, ImmutableSet.of( emailDomain ) );
        return null;
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
        return getAuthorizedRoles( organizationId, Permission.READ );
    }

    private Set<Role> getAuthorizedRoles( UUID organizationId, Permission permission ) {
        return StreamUtil.stream( organizations.getRoles( organizationId ) )
                .filter( role -> isAuthorized( permission ).test( role.getAclKey() ) )
                .collect( Collectors.toSet() );
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
    @DeleteMapping(
            value = ID_PATH + PRINCIPALS + ROLES + ROLE_ID_PATH )
    public Void deleteRole( @PathVariable( ID ) UUID organizationId, @PathVariable( ROLE_ID ) UUID roleId ) {
        ensureRoleAdminAccess( organizationId, roleId );
        principalService.deletePrincipal( new AclKey( organizationId, roleId ) );
        return null;
    }

    @Timed
    @Override
    @GetMapping(
            value = ID_PATH + PRINCIPALS + ROLES + ROLE_ID_PATH + MEMBERS,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public Iterable<Auth0UserBasic> getAllUsersOfRole(
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

    private AclKey ensureRead( UUID organizationId ) {
        AclKey aclKey = new AclKey( organizationId );
        accessCheck( aclKey, EnumSet.of( Permission.READ ) );
        return aclKey;
    }

    private AclKey ensureMaterialize ( UUID entitySetId, OrganizationPrincipal principal ) {
        AclKey aclKey = new AclKey( entitySetId );

        if ( !getAuthorizationManager().checkIfHasPermissions(
                aclKey,
                Set.of(principal.getPrincipal()),
                EnumSet.of( Permission.MATERIALIZE ) ) ) {
            throw new ForbiddenException( "EntitySet " + aclKey.toString() + " is not accessible by organization " +
                    "principal " + principal.getPrincipal().getId()  + " ." );
        }

        return aclKey;
    }

}
