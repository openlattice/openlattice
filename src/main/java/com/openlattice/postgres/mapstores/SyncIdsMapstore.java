package com.openlattice.postgres.mapstores;

import static com.openlattice.postgres.PostgresColumn.CURRENT_SYNC_ID;
import static com.openlattice.postgres.PostgresColumn.ENTITY_SET_ID;
import static com.openlattice.postgres.PostgresColumn.SYNC_ID;
import static com.openlattice.postgres.PostgresTable.SYNC_IDS;

import com.openlattice.hazelcast.HazelcastMap;
import com.google.common.collect.ImmutableList;
import com.openlattice.postgres.PostgresColumnDefinition;
import com.openlattice.postgres.ResultSetAdapters;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SyncIdsMapstore extends AbstractBasePostgresMapstore<UUID, UUID> {
    private final HikariDataSource hds;

    public SyncIdsMapstore( HikariDataSource hds ) {
        super( HazelcastMap.SYNC_IDS.name(), SYNC_IDS, hds );
        this.hds = hds;
    }

    @Override protected String buildInsertQuery() {
        return "UPDATE ".concat( SYNC_IDS.getName() ).concat( " SET " )
                .concat( CURRENT_SYNC_ID.getName() ).concat( " = ? WHERE " ).concat( ENTITY_SET_ID.getName() )
                .concat( " = ?;" );
    }

    @Override protected List<PostgresColumnDefinition> initKeyColumns() {
        return ImmutableList.of( ENTITY_SET_ID );
    }

    @Override protected List<PostgresColumnDefinition> initValueColumns() {
        return ImmutableList.of( SYNC_ID, CURRENT_SYNC_ID );
    }

    @Override protected void bind( PreparedStatement ps, UUID key, UUID value ) throws SQLException {
        bind( ps, key, 2 );
        ps.setObject( 1, value );
    }

    @Override protected int bind( PreparedStatement ps, UUID key, int parameterIndex ) throws SQLException {
        ps.setObject( parameterIndex++, key );
        return parameterIndex;
    }

    @Override protected UUID mapToValue( ResultSet rs ) throws SQLException {
        return ResultSetAdapters.currentSyncId( rs );
    }

    @Override protected UUID mapToKey( ResultSet rs ) throws SQLException {
        return ResultSetAdapters.entitySetId( rs );
    }

    //TODO: Fix this.
    // This mapstore should never add rows to the existing table -- instead it should just update
    // existing rows when a new currentSyncId is set for an entity set

    @Override
    public Map<UUID, UUID> loadAll( Collection<UUID> keys ) {
        return keys.parallelStream().distinct()
                .collect( Collectors.toConcurrentMap( Function.identity(), this::load ) );
    }

    @Override public UUID generateTestKey() {
        return UUID.randomUUID();
    }

    @Override public UUID generateTestValue() {
        return UUID.randomUUID();
    }
}
