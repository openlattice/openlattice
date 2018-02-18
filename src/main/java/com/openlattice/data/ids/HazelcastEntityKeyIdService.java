

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

package com.openlattice.data.ids;

import com.codahale.metrics.annotation.Timed;
import com.dataloom.hazelcast.ListenableHazelcastFuture;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.openlattice.data.EntityKey;
import com.openlattice.data.EntityKeyIdService;
import com.openlattice.data.mapstores.PostgresEntityKeyIdsMapstore;
import com.openlattice.datastore.util.Util;
import com.openlattice.hazelcast.HazelcastMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Most of the logic for this class is handled by the map store, which ensures a unique id
 * is assigned on read.
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class HazelcastEntityKeyIdService implements EntityKeyIdService {
    private static final Logger logger = LoggerFactory.getLogger( HazelcastEntityKeyIdService.class );
    private final ListeningExecutorService executor;

    private final IMap<EntityKey, UUID> ids;

    public HazelcastEntityKeyIdService(
            HazelcastInstance hazelcastInstance,
            ListeningExecutorService executor ) {
        this.ids = hazelcastInstance.getMap( HazelcastMap.IDS.name() );
        this.executor = executor;
    }

    @Override
    @Timed
    public EntityKey getEntityKey( UUID entityKeyId ) {
        Set<EntityKey> entityKeys = ids.keySet( Predicates.equal( PostgresEntityKeyIdsMapstore.ID, entityKeyId ) );
        if ( entityKeys == null || entityKeys.isEmpty() ) {
            return null;
        }
        return entityKeys.iterator().next();
    }

    @Override
    @Timed
    public Map<UUID, EntityKey> getEntityKeys( Set<UUID> entityKeyIds ) {
        Set<Entry<EntityKey, UUID>> entries = getEntityKeyEntries( entityKeyIds );
        return entries
                .stream()
                .collect( Collectors.toMap( Entry::getValue, Entry::getKey ) );
    }

    @Override
    @Timed
    public UUID getEntityKeyId( EntityKey entityKey ) {
        return Util.getSafely( ids, entityKey );
    }

    @Override
    @Timed
    public Map<EntityKey, UUID> getEntityKeyIds( Set<EntityKey> entityKeys ) {
        return Util.getSafely( ids, entityKeys );
    }

    @Timed
    @Override
    public Set<Entry<EntityKey, UUID>> getEntityKeyEntries( Set<UUID> entityKeyIds ) {
        Predicate<EntityKey, UUID> p = Predicates
                .in( PostgresEntityKeyIdsMapstore.ID, entityKeyIds.toArray( new UUID[ 0 ] ) );
        return ids.entrySet( p );
    }

    public Stream<EntityKey> getEntityKeysInEntitySet() {
        return null;
    }

    @Override
    public ListenableFuture<UUID> getEntityKeyIdAsync( EntityKey entityKey ) {
        return new ListenableHazelcastFuture<>( ids.getAsync( entityKey ) );
    }

}
