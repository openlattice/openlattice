

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

import com.hazelcast.query.Predicates;
import com.openlattice.authorization.securable.SecurableObjectType;
import com.openlattice.hazelcast.HazelcastMap;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.openlattice.postgres.mapstores.SecurableObjectTypeMapstore;
import java.util.Set;

public class HazelcastSecurableObjectResolveTypeService implements SecurableObjectResolveTypeService {

    private final IMap<AclKey, SecurableObjectType> securableObjectTypes;

    public HazelcastSecurableObjectResolveTypeService( HazelcastInstance hazelcastInstance ) {
        securableObjectTypes = hazelcastInstance.getMap( HazelcastMap.SECURABLE_OBJECT_TYPES.name() );
    }

    @Override
    public void createSecurableObjectType( AclKey aclKey, SecurableObjectType type ) {
        securableObjectTypes.set( new AclKey( aclKey ), type );

    }

    @Override
    public void deleteSecurableObjectType( AclKey aclKey ) {
        securableObjectTypes.remove( new AclKey( aclKey ) );
    }

    @Override
    public SecurableObjectType getSecurableObjectType( AclKey aclKey ) {
        return securableObjectTypes.get( new AclKey( aclKey ) );
    }

    @Override
    public Set<AclKey> getSecurableObjectsOfType( SecurableObjectType type ) {
        return securableObjectTypes
                .keySet( Predicates.equal( SecurableObjectTypeMapstore.SECURABLE_OBJECT_TYPE_INDEX, type ) );
    }
}
