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

import static com.openlattice.bootstrap.AuthorizationBootstrap.GLOBAL_ADMIN_ROLE;
import static com.openlattice.bootstrap.AuthorizationBootstrap.OPENLATTICE_ROLE;

import com.google.common.collect.ImmutableSet;
import com.openlattice.authorization.Principal;
import com.openlattice.authorization.PrincipalType;
import com.openlattice.organization.Organization;
import com.openlattice.organizations.HazelcastOrganizationService;
import java.util.Optional;
import java.util.UUID;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class OrganizationBootstrap {
    public static final Organization OPENLATTICE = createOpenLatticeOrg();
    public static final Organization GLOBAL      = createGlobalOrg();
    private boolean initialized;

    public OrganizationBootstrap( HazelcastOrganizationService organizationService ) {
        organizationService.createOrganization( OPENLATTICE_ROLE.getPrincipal(), OPENLATTICE );
        organizationService.createOrganization( GLOBAL_ADMIN_ROLE.getPrincipal(), GLOBAL );
        initialized = true;
    }

    public boolean isInitialized() {
        return initialized;
    }

    public static Organization createGlobalOrg() {
        UUID id = BootstrapConstants.GLOBAL_ORGANIZATION_ID;
        Principal org = new Principal( PrincipalType.ORGANIZATION, "globalOrg" );
        String title = "Global Organization";
        return new Organization( Optional.of( id ),
                org,
                title,
                Optional.empty(),
                ImmutableSet.of(),
                ImmutableSet.of(),
                ImmutableSet.of() );
    }

    public static Organization createOpenLatticeOrg() {
        UUID id = new UUID( 0, 0 );
        Principal org = new Principal( PrincipalType.ORGANIZATION, "openlatticeOrg" );
        String title = "OpenLattice, Inc.";
        return new Organization( Optional.of( id ),
                org,
                title,
                Optional.empty(),
                ImmutableSet.of(),
                ImmutableSet.of(),
                ImmutableSet.of() );
    }
}
