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

import java.util.UUID;

import com.openlattice.hazelcast.HazelcastMap;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.openlattice.datastore.util.Util;

public class HazelcastVertexMergingService {
    private IMap<LinkingVertexKey, UUID> newIds;

    public HazelcastVertexMergingService( HazelcastInstance hazelcastInstance ) {
        this.newIds = hazelcastInstance.getMap( HazelcastMap.VERTEX_IDS_AFTER_LINKING.name() );
    }

    public void saveToLookup( UUID graphId, UUID oldId, UUID newId ){
        newIds.put( new LinkingVertexKey( graphId, oldId ), newId );
    }
    
    public UUID getMergedId( UUID graphId, UUID oldId ){
        return Util.getSafely( newIds, new LinkingVertexKey( graphId, oldId ) );
    }
}
