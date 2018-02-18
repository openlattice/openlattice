

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

package com.openlattice.linking;

import java.util.Set;
import java.util.UUID;

import com.openlattice.hazelcast.HazelcastMap;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.openlattice.datastore.util.Util;
import com.openlattice.rhizome.hazelcast.DelegatedUUIDSet;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class HazelcastListingService {
    private final IMap<UUID, DelegatedUUIDSet> linkedEntityTypes;
    private final IMap<UUID, DelegatedUUIDSet> linkedEntitySets;

    public HazelcastListingService( HazelcastInstance hazelcastInstance ) {
        this.linkedEntityTypes = hazelcastInstance.getMap( HazelcastMap.LINKED_ENTITY_TYPES.name() );
        this.linkedEntitySets = hazelcastInstance.getMap( HazelcastMap.LINKED_ENTITY_SETS.name() );
    }

    public void setLinkedEntityTypes( UUID entityTypeId, Set<UUID> entityTypes ) {
        this.linkedEntityTypes.set( entityTypeId, DelegatedUUIDSet.wrap( entityTypes ) );
    }

    public boolean isLinkedEntityType( UUID entityTypeId ) {
        return linkedEntityTypes.containsKey( entityTypeId );
    }

    public void setLinkedEntitySets( UUID entitySetId, Set<UUID> entitySets ) {
        this.linkedEntitySets.set( entitySetId, DelegatedUUIDSet.wrap( entitySets ) );
    }
    
    public DelegatedUUIDSet getLinkedEntitySets( UUID entitySetId ) {
        return Util.getSafely( linkedEntitySets, entitySetId );
    }

    public boolean isLinkedEntitySet( UUID entitySetId ) {
        return linkedEntitySets.containsKey( entitySetId );
    }

}
