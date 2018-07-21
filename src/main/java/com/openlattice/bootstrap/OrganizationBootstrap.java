/*
 * Copyright (C) 2017. OpenLattice, Inc
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

package com.openlattice.bootstrap;

import static com.google.common.base.Preconditions.checkState;
import static com.openlattice.bootstrap.AuthorizationBootstrap.GLOBAL_ADMIN_ROLE;
import static com.openlattice.bootstrap.AuthorizationBootstrap.OPENLATTICE_ROLE;

import com.google.common.collect.ImmutableSet;
import com.openlattice.authorization.Principal;
import com.openlattice.authorization.PrincipalType;
import com.openlattice.organization.Organization;
import com.openlattice.organizations.HazelcastOrganizationService;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class OrganizationBootstrap {
    public static final  Principal GLOBAL_ORG_PRINCIPAL      = new Principal( PrincipalType.ORGANIZATION, "globalOrg" );
    public static final  Principal OPENLATTICE_ORG_PRINCIPAL = new Principal( PrincipalType.ORGANIZATION,
            "openlatticeOrg" );
    private static final Logger    logger                    = LoggerFactory.getLogger( OrganizationBootstrap.class );
    private              boolean   initialized;

    public OrganizationBootstrap( HazelcastOrganizationService organizationService ) {
        var globalOrg = organizationService.maybeGetOrganization( GLOBAL_ORG_PRINCIPAL );
        var olOrg = organizationService.maybeGetOrganization( GLOBAL_ORG_PRINCIPAL );

        if ( globalOrg.isPresent() ) {
            logger.info( "Expected id = {}, Actual id = {}",
                    BootstrapConstants.GLOBAL_ORGANIZATION_ID,
                    globalOrg.get().getId() );
            checkState( BootstrapConstants.GLOBAL_ORGANIZATION_ID.equals( globalOrg.get().getId() ) );
        } else {
            organizationService.createOrganization( GLOBAL_ADMIN_ROLE.getPrincipal(), createGlobalOrg() );
        }

        if ( olOrg.isPresent() ) {
            logger.info( "Expected id = {}, Actual id = {}",
                    BootstrapConstants.OPENLATTICE_ORGANIZATION_ID,
                    olOrg.get().getId() );
            checkState( BootstrapConstants.OPENLATTICE_ORGANIZATION_ID.equals( olOrg.get().getId() ) );
        } else {
            organizationService.createOrganization( OPENLATTICE_ROLE.getPrincipal(), createOpenLatticeOrg() );
        }

        initialized = true;
    }

    public boolean isInitialized() {
        return initialized;
    }

    private static Organization createGlobalOrg() {
        UUID id = BootstrapConstants.GLOBAL_ORGANIZATION_ID;
        String title = "Global Organization";
        return new Organization( Optional.of( id ),
                GLOBAL_ORG_PRINCIPAL,
                title,
                Optional.empty(),
                ImmutableSet.of(),
                ImmutableSet.of(),
                ImmutableSet.of() );
    }

    private static Organization createOpenLatticeOrg() {
        UUID id = BootstrapConstants.OPENLATTICE_ORGANIZATION_ID;
        String title = "OpenLattice, Inc.";
        return new Organization( Optional.of( id ),
                OPENLATTICE_ORG_PRINCIPAL,
                title,
                Optional.empty(),
                ImmutableSet.of(),
                ImmutableSet.of(),
                ImmutableSet.of() );
    }
}
