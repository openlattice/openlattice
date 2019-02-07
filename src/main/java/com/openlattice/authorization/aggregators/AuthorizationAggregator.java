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

package com.openlattice.authorization.aggregators;

import static com.google.common.base.Preconditions.checkNotNull;

import com.hazelcast.aggregation.Aggregator;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Map.Entry;

import com.openlattice.authorization.AceKey;
import com.openlattice.authorization.AceValue;
import com.openlattice.authorization.AclKey;
import com.openlattice.authorization.Permission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class AuthorizationAggregator extends Aggregator<Entry<AceKey, AceValue>, AuthorizationAggregator> {
    private static final Logger logger = LoggerFactory.getLogger( AuthorizationAggregator.class );
    //    private final EnumMap<Permission, Boolean> permissions;
    private final Map<AclKey, EnumMap<Permission, Boolean>> permissionsMap;

    public AuthorizationAggregator( Map<AclKey, EnumMap<Permission, Boolean>> permissionsMap ) {
        this.permissionsMap = permissionsMap;
    }

    @Override public void accumulate( Entry<AceKey, AceValue> input ) {
        AclKey aclKey = input.getKey().getAclKey();

        EnumMap<Permission, Boolean> permissions = permissionsMap.get( aclKey );

        final EnumSet<Permission> acePermissions = checkNotNull(
                input.getValue().getPermissions(),
                "Permissions shouldn't be null" );

        for ( Permission p : acePermissions ) {
            permissions.computeIfPresent( p, ( k, v ) -> true );
        }

    }

    @Override public void combine( Aggregator aggregator ) {
        //A class cast exception = madness
        AuthorizationAggregator other = (AuthorizationAggregator) aggregator;
        //They should be the same keysets

        for ( Entry<AclKey, EnumMap<Permission, Boolean>> permissionEntry : permissionsMap.entrySet() ) {
            EnumMap<Permission, Boolean> permissions = permissionEntry.getValue();
            EnumMap<Permission, Boolean> otherPermissions = other.permissionsMap.get( permissionEntry.getKey() );
            for ( Entry<Permission, Boolean> e : otherPermissions.entrySet() ) {
                Permission p = e.getKey();
                permissions.put( p, permissions.get( p ).booleanValue() || e.getValue().booleanValue() );
            }
        }
    }

    public Map<AclKey, EnumMap<Permission, Boolean>> getPermissions() {
        return permissionsMap;
    }

    @Override public AuthorizationAggregator aggregate() {
        return this;
    }

    @Override public boolean equals( Object o ) {
        if ( this == o ) { return true; }
        if ( !( o instanceof AuthorizationAggregator ) ) { return false; }

        AuthorizationAggregator that = (AuthorizationAggregator) o;

        return permissionsMap.equals( that.permissionsMap );
    }

    @Override public int hashCode() {
        return permissionsMap.hashCode();
    }

    @Override public String toString() {
        return "AuthorizationAggregator{" +
                "permissionsMap=" + permissionsMap +
                '}';
    }
}
