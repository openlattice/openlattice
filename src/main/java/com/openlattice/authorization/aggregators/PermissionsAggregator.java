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

package com.openlattice.authorization.aggregators;

import com.openlattice.authorization.AceKey;
import com.openlattice.authorization.Permission;
import com.openlattice.authorization.Principal;
import com.openlattice.authorization.securable.SecurableObjectType;
import com.google.common.collect.Sets;
import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.openlattice.authorization.AceValue;
import com.openlattice.authorization.AclKey;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Map;
import java.util.Set;

public class PermissionsAggregator extends Aggregator<Map.Entry<AceKey, AceValue>, Set<AclKey>>
        implements HazelcastInstanceAware {
    private static final long serialVersionUID = -1015754054455567010L;

    private final SecurableObjectType objectType;
    @SuppressFBWarnings( value = "SE_BAD_FIELD", justification = "This class is unused and will need a stream serializer if used." )
    private final Set<Principal>      principals;
    private final Set<Permission>     permissions;

    @SuppressFBWarnings( value = "SE_BAD_FIELD", justification = "This class is unused and will need a stream serializer if used." )
    private Set<AclKey> result;

    public PermissionsAggregator(
            SecurableObjectType objectType,
            Set<Principal> principals,
            Set<Permission> permissions ) {
        this( objectType, principals, permissions, Sets.newHashSet() );
    }

    public PermissionsAggregator(
            SecurableObjectType objectType,
            Set<Principal> principals,
            Set<Permission> permissions,
            Set<AclKey> result ) {
        this.objectType = objectType;
        this.principals = principals;
        this.permissions = permissions;
        this.result = result;
    }

    @Override public void accumulate( Map.Entry<AceKey, AceValue> input ) {

    }

    @Override public void combine( Aggregator aggregator ) {
        if ( aggregator instanceof PermissionsAggregator ) {
            result.addAll( ( (PermissionsAggregator) aggregator ).result );
        }
    }

    @Override public Set<AclKey> aggregate() {
        return result;
    }

    @Override public void setHazelcastInstance( HazelcastInstance hazelcastInstance ) {

    }

    public SecurableObjectType getObjectType() {
        return objectType;
    }

    public Set<Principal> getPrincipals() {
        return principals;
    }

    public Set<Permission> getPermissions() {
        return permissions;
    }

    public Set<AclKey> getResult() {
        return result;
    }
}
