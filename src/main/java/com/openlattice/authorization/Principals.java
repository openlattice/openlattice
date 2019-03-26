

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
import static com.google.common.base.Preconditions.checkState;

import com.openlattice.organizations.PrincipalSet;
import com.openlattice.organizations.roles.SecurePrincipalsManager;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.util.Collection;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;

public final class Principals {
    private static final Logger logger = LoggerFactory.getLogger( Principals.class );
    private static final Lock startupLock = new ReentrantLock();
    private static LoadingCache<String, SecurablePrincipal>      users;
    private static LoadingCache<String, NavigableSet<Principal>> principals;

    private Principals() {
    }

    public static void init( SecurePrincipalsManager spm ) {
        if ( startupLock.tryLock() ) {
            users = CacheBuilder
                    .newBuilder()
                    .expireAfterWrite( 1, TimeUnit.SECONDS )
                    .build( new CacheLoader<String, SecurablePrincipal>() {
                        @Override public SecurablePrincipal load( String principalId ) throws Exception {
                            return spm.getPrincipal( principalId );
                        }
                    } );

            principals = CacheBuilder
                    .newBuilder()
                    .expireAfterWrite( 30, TimeUnit.SECONDS )
                    .build( new CacheLoader<String, NavigableSet<Principal>>() {
                        @Override public NavigableSet<Principal> load( String principalId ) throws Exception {
                            SecurablePrincipal sp = users.getUnchecked( principalId );
                            Collection<SecurablePrincipal> securablePrincipals = spm.getAllPrincipals( sp );
                            if ( securablePrincipals == null ) {
                                return null;
                            }
                            NavigableSet<Principal> currentPrincipals = new TreeSet<>();
                            currentPrincipals.add( sp.getPrincipal() );
                            securablePrincipals.stream().map( SecurablePrincipal::getPrincipal )
                                    .forEach( currentPrincipals::add );
                            return currentPrincipals;
                        }
                    } );
        } else {
            logger.error( "Principals security processing can only be initialized once." );
            throw new IllegalStateException( "Principals context already initialized." );
        }
    }

    public static void requireOrganization( Principal principal ) {
        checkArgument( principal.getType().equals( PrincipalType.ORGANIZATION ) );
    }

    public static void ensureUser( Principal principal ) {
        checkState( principal.getType().equals( PrincipalType.USER ), "Only user principal type allowed." );
    }

    /**
     * This will retrieve the current user. If auth information isn't present an NPE is thrown (by design). If the wrong
     * type of auth is present a ClassCast exception will be thrown (by design).
     *
     * @return The principal for the current request.
     */
    public static @Nonnull Principal getCurrentUser() {
        return getUserPrincipal( getCurrentPrincipalId() );
    }

    public static SecurablePrincipal getCurrentSecurablePrincipal() {
        return users.getUnchecked( getCurrentPrincipalId() );
    }

    public static Principal getUserPrincipal( String principalId ) {
        return new Principal( PrincipalType.USER, principalId );
    }

    public static NavigableSet<Principal> getUserPrincipals( String principalId ) {
        return principals.getUnchecked( principalId );
    }

    public static NavigableSet<Principal> getCurrentPrincipals() {
        return principals.getUnchecked( getCurrentPrincipalId() );
    }

    public static Principal getAdminRole() {
        return SystemRole.ADMIN.getPrincipal();
    }

    public static SimpleGrantedAuthority fromPrincipal( Principal p ) {
        return new SimpleGrantedAuthority( p.getType().name() + "|" + p.getId() );
    }

    private static String getPrincipalId( Authentication authentication ) {
        return authentication.getPrincipal().toString();
    }

    private static String getCurrentPrincipalId() {
        return getPrincipalId( SecurityContextHolder.getContext().getAuthentication() );
    }

}
