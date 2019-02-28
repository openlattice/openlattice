

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

package com.openlattice.authentication;

import com.auth0.spring.security.api.authentication.JwtAuthentication;
import com.openlattice.authorization.Principal;
import com.openlattice.authorization.PrincipalType;
import com.openlattice.authorization.Principals;
import com.openlattice.controllers.exceptions.ForbiddenException;
import com.openlattice.organizations.roles.SecurePrincipalsManager;
import com.openlattice.authorization.SecurablePrincipal;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Collection;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;

public class WrappedAuth0Authentication implements Authentication {

    private static final Logger logger           = LoggerFactory.getLogger( WrappedAuth0Authentication.class );
    private static final long   serialVersionUID = -6853527586490225640L;

    private final Principal                   principal;
    private final NavigableSet<Principal>     principals;
    private final Set<SimpleGrantedAuthority> grantedAuthorities;
    private final Authentication              jwtToken;

    public WrappedAuth0Authentication( Authentication authentication, SecurePrincipalsManager spm ) {
        if ( authentication != null
                && JwtAuthentication.class.isAssignableFrom( authentication.getClass() )
                && authentication.isAuthenticated() ) {

            jwtToken = authentication;

            String principalId = authentication.getPrincipal().toString();
            principal = new Principal( PrincipalType.USER, principalId.toString() );

            SecurablePrincipal sp = spm.getPrincipal( principal.getId() );
            Collection<SecurablePrincipal> securablePrincipals = spm.getAllPrincipals( sp );

            principals = new TreeSet<>();
            principals.add( principal );

            grantedAuthorities =
                    securablePrincipals
                            .stream()
                            .map( SecurablePrincipal::getPrincipal )
                            .peek( principals::add )
                            .map( Principals::fromPrincipal )
                            .collect( Collectors.toSet() );

        } else {
            logger.debug( "Authentication failed for authentication: {}", authentication );
            throw new ForbiddenException( "Unable to authorize access to requested resource." );
        }
    }

    public Principal getLoomPrincipal() {
        return principal;
    }

    public NavigableSet<Principal> getLoomPrincipals() {
        return principals;
    }

    @Override
    public Collection<? extends GrantedAuthority> getAuthorities() {
        return grantedAuthorities;
    }

    @Override
    public Object getPrincipal() {
        return jwtToken.getPrincipal();
    }

    public Object getCredentials() {
        return jwtToken.getCredentials();
    }

    public String getName() {
        return jwtToken.getName();
    }

    public boolean isAuthenticated() {
        return jwtToken.isAuthenticated();
    }

    public void setAuthenticated( boolean authenticated ) {
        jwtToken.setAuthenticated( authenticated );
    }

    public Object getDetails() {
        return jwtToken.getDetails();
    }

    @SuppressFBWarnings
    public boolean equals( Object obj ) {

        if ( obj == null ) {
            return false;
        }

        if ( !( obj instanceof WrappedAuth0Authentication ) ) {
            return false;
        }

        WrappedAuth0Authentication test = (WrappedAuth0Authentication) obj;

        if ( this.principal == null && test.principal != null ) {
            return false;
        }

        if ( this.principal != null && test.principal == null ) {
            return false;
        }

        return this.principal != null && this.principal.equals( test.principal );
    }

    public int hashCode() {
        return jwtToken.hashCode();
    }

    public String toString() {
        return jwtToken.toString();
    }

}
