/*
 * Copyright (C) 2018. OpenLattice, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * You can contact the owner of the copyright at support@openlattice.com
 */

package com.openlattice.authorization;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public enum SystemRole {
    ADMIN( "admin" ),
    USER( "user" ),
    AUTHENTICATED_USER( "AuthenticatedUser" ),
    OPENLATTICE( "openlatticeRole" );
    private final Principal          principal;
    
    private static final Set<String> allRoles;

    static {
        allRoles = Stream.of( values() ).map( role -> role.getName() ).collect( Collectors.toSet() );
    }

    private SystemRole( String principalId ) {
        this.principal = new Principal( PrincipalType.ROLE, principalId );
    }

    public Principal getPrincipal() {
        return principal;
    }

    public String getName() {
        return principal.getId();
    }
    
    public static boolean contains( String role ) {
        return allRoles.contains( role );
    }
    
    public static String[] valuesAsArray() {
        return allRoles.toArray( new String[ allRoles.size() ] );
    }
};