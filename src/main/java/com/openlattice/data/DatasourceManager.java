

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

package com.openlattice.data;

import com.codahale.metrics.annotation.Timed;
import com.dataloom.streams.StreamUtil;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.eventbus.EventBus;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.query.Predicate;
import com.openlattice.data.hazelcast.DataKey;
import com.openlattice.data.hazelcast.EntitySets;
import com.openlattice.edm.EntitySet;
import com.openlattice.hazelcast.HazelcastMap;
import com.openlattice.neuron.audit.AuditEntitySetUtils;
import com.openlattice.postgres.PostgresColumn;
import com.openlattice.postgres.PostgresTable;
import com.openlattice.postgres.ResultSetAdapters;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

import javax.inject.Inject;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

public class DatasourceManager {
    private static final Logger logger = LoggerFactory.getLogger( DatasourceManager.class );
    private final HikariDataSource          hds;
    private final IMap<UUID, UUID>          currentSyncIds;
    private final IMap<DataKey, ByteBuffer> data;
    private final IMap<EntityKey, UUID>     ids;
    private final IMap<UUID, EntitySet>     entitySets;

    private final String mostRecentSyncIdSql;
    private final String writeSyncIdsSql;
    private final String allSyncIdsSql;
    private final String allPreviousEntitySetsAndSyncIdsSql;
    private final String allCurrentSyncIdsSql;

    @Inject
    private EventBus eventBus;

    public DatasourceManager( HikariDataSource hds, HazelcastInstance hazelcastInstance ) {
        this.hds = hds;

        this.currentSyncIds = hazelcastInstance.getMap( HazelcastMap.SYNC_IDS.name() );
        this.data = hazelcastInstance.getMap( HazelcastMap.DATA.name() );
        this.ids = hazelcastInstance.getMap( HazelcastMap.IDS.name() );
        this.entitySets = hazelcastInstance.getMap( HazelcastMap.ENTITY_SETS.name() );

        // Tables
        String SYNC_IDS = PostgresTable.SYNC_IDS.getName();

        // Columns
        String ENTITY_SET_ID = PostgresColumn.ENTITY_SET_ID.getName();
        String SYNC_ID = PostgresColumn.SYNC_ID.getName();
        String CURRENT_SYNC_ID = PostgresColumn.CURRENT_SYNC_ID.getName();

        this.mostRecentSyncIdSql = "SELECT " .concat( SYNC_ID ).concat( " FROM " ).concat( SYNC_IDS )
                .concat( " WHERE " )
                .concat( ENTITY_SET_ID ).concat( " = ? ORDER BY " ).concat( SYNC_ID ).concat( " DESC LIMIT 1;" );
        this.writeSyncIdsSql = "INSERT INTO " .concat( SYNC_IDS ).concat( " (" ).concat( ENTITY_SET_ID ).concat( ", " )
                .concat( SYNC_ID ).concat( ", " ).concat( CURRENT_SYNC_ID ).concat( ") VALUES ( ?, ?, ? );" );
        this.allSyncIdsSql = "SELECT " .concat( SYNC_ID ).concat( " FROM " ).concat( SYNC_IDS )
                .concat( " WHERE " ).concat( ENTITY_SET_ID ).concat( " = ?;" );
        this.allPreviousEntitySetsAndSyncIdsSql = "SELECT " .concat( ENTITY_SET_ID ).concat( ", " ).concat( SYNC_ID )
                .concat( " FROM " ).concat( SYNC_IDS ).concat( " WHERE " )
                .concat( ENTITY_SET_ID ).concat( " = ? AND " ).concat( SYNC_ID ).concat( " < ?;" );
        this.allCurrentSyncIdsSql = "SELECT DISTINCT " .concat( ENTITY_SET_ID ).concat( ", " ).concat( CURRENT_SYNC_ID )
                .concat( " FROM " ).concat( SYNC_IDS ).concat( ";" );
    }

    public UUID getCurrentSyncId( UUID entitySetId ) {
        return currentSyncIds.get( entitySetId );
    }

    public Map<UUID, UUID> getCurrentSyncId( Set<UUID> entitySetIds ) {
        return currentSyncIds.getAll( entitySetIds );
    }

    @Deprecated
    public void setCurrentSyncId( UUID entitySetId, UUID syncId ) {

        currentSyncIds.put( entitySetId, syncId );

        if ( entitySetId.equals( AuditEntitySetUtils.getId() ) ) {
            AuditEntitySetUtils.setSyncId( syncId );
        }
    }

    @Deprecated
    public UUID createNewSyncIdForEntitySet( UUID entitySetId ) {
        UUID newSyncId = new UUID( System.currentTimeMillis(), 0 );
        addSyncIdToEntitySet( entitySetId, newSyncId );
        return newSyncId;
    }

    public UUID getLatestSyncId( UUID entitySetId ) {
        try ( Connection connection = hds.getConnection();
                PreparedStatement ps = connection.prepareStatement( mostRecentSyncIdSql ) ) {
            UUID result = null;
            ps.setObject( 1, entitySetId );

            ResultSet rs = ps.executeQuery();
            if ( rs.next() ) {
                result = ResultSetAdapters.syncId( rs );
            }
            rs.close();
            connection.close();
            return result;
        } catch ( SQLException e ) {
            logger.debug( "Unable to load most recent sync id for entity set id: {}", entitySetId, e );
            return null;
        }
    }

    public Iterable<UUID> getAllPreviousSyncIds( UUID entitySetId, UUID syncId ) {
        try ( Connection connection = hds.getConnection();
                PreparedStatement ps = connection.prepareStatement( allPreviousEntitySetsAndSyncIdsSql ) ) {
            List<UUID> result = Lists.newArrayList();
            ps.setObject( 1, entitySetId );
            ps.setObject( 2, syncId );

            ResultSet rs = ps.executeQuery();
            while ( rs.next() ) {
                result.add( ResultSetAdapters.syncId( rs ) );
            }
            connection.close();
            return result;

        } catch ( SQLException e ) {
            logger.debug( "Unable to load all previous sync ids for entity set id: {}", entitySetId, e );
            return ImmutableList.of();
        }
    }

    public Iterable<UUID> getAllSyncIds( UUID entitySetId ) {
        try ( Connection connection = hds.getConnection();
                PreparedStatement ps = connection.prepareStatement( allSyncIdsSql ) ) {
            List<UUID> result = Lists.newArrayList();
            ps.setObject( 1, entitySetId );

            ResultSet rs = ps.executeQuery();
            while ( rs.next() ) {
                result.add( ResultSetAdapters.syncId( rs ) );
            }
            connection.close();
            return result;

        } catch ( SQLException e ) {
            logger.debug( "Unable to load all previous sync ids for entity set id: {}", entitySetId, e );
            return ImmutableList.of();
        }
    }

    private void addSyncIdToEntitySet( UUID entitySetId, UUID syncId ) {
        try ( Connection connection = hds.getConnection();
                PreparedStatement ps = connection.prepareStatement( writeSyncIdsSql ) ) {
            ps.setObject( 1, entitySetId );
            ps.setObject( 2, syncId );
            ps.setObject( 3, currentSyncIds.get( entitySetId ) );
            ps.execute();
            connection.close();
            return;

        } catch ( SQLException e ) {
            logger.debug( "Unable to add sync id {} to entity set id: {}", syncId, entitySetId, e );
            return;
        }
    }

    @Scheduled( fixedRate = 60000 )
    public void reap() {
        logger.info( "Reaping old syncs from memory" );
        cleanup();
    }

    private List<List<UUID>> getAllPreviousEntitySetSyncIdPairs( Map.Entry<UUID, UUID> entry ) {
        try ( Connection connection = hds.getConnection();
                PreparedStatement ps = connection.prepareStatement( allPreviousEntitySetsAndSyncIdsSql ) ) {
            List<List<UUID>> result = Lists.newArrayList();
            ps.setObject( 1, entry.getKey() );
            ps.setObject( 2, entry.getValue() );
            ResultSet rs = ps.executeQuery();
            while ( rs.next() ) {
                UUID entitySetId = ResultSetAdapters.entitySetId( rs );
                UUID syncId = ResultSetAdapters.syncId( rs );
                result.add( ImmutableList.of( entitySetId, syncId ) );
            }
            connection.close();
            return result;

        } catch ( SQLException e ) {
            logger.debug( "Unable to load all previous entity set sync id pairs for entity set {} and sync id: {}",
                    entry.getKey(),
                    entry.getValue(),
                    e );
            return ImmutableList.of();
        }
    }

    @Timed
    public void cleanup() {
        //This will evict as opposed to remove all items.
        List<Predicate> predicates = StreamUtil.stream( currentSyncIds.entrySet() )
                .map( this::getAllPreviousEntitySetSyncIdPairs )
                .flatMap( StreamUtil::stream )
                .map( EntitySets::filterByEntitySetIdAndSyncId )
                .collect( Collectors.toList() );
        predicates.stream()
                .map( data::keySet )
                .flatMap( Set::stream )
                .forEach( data::evict );
        predicates.stream()
                .map( ids::keySet )
                .flatMap( Set::stream )
                .forEach( ids::evict );
    }
}
