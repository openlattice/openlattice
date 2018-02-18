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

package com.openlattice.data.hazelcast;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.openlattice.data.EntityKey;
import com.openlattice.data.HazelcastStream;
import com.openlattice.data.aggregators.EntityKeyAggregator;
import com.openlattice.hazelcast.HazelcastMap;
import java.util.UUID;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class EntityKeyHazelcastStream extends HazelcastStream<EntityKey> {
    private final IMap<EntityKey, UUID> ids;
    private final UUID                  entitySetId;
    private final UUID                  syncId;

    public EntityKeyHazelcastStream(
            ListeningExecutorService executor,
            HazelcastInstance hazelcastInstance, UUID entitySetId, UUID syncId ) {
        super( executor, hazelcastInstance );
        this.ids = hazelcastInstance.getMap( HazelcastMap.IDS.name() );
        this.entitySetId = entitySetId;
        this.syncId = syncId;
    }

    @Override
    protected long buffer( UUID streamId ) {
        return ids.aggregate(
                new EntityKeyAggregator( streamId ),
                EntitySets.filterByEntitySetIdAndSyncId( entitySetId, syncId ) );
    }
}
