package com.openlattice.postgres.mapstores;

import static com.openlattice.postgres.PostgresTable.ENTITY_SETS;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapIndexConfig;
import com.openlattice.edm.EntitySet;
import com.openlattice.edm.set.EntitySetFlag;
import com.openlattice.hazelcast.HazelcastMap;
import com.openlattice.mapstores.TestDataFactory;
import com.openlattice.postgres.PostgresArrays;
import com.openlattice.postgres.ResultSetAdapters;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.*;
import java.util.UUID;

public class EntitySetMapstore extends AbstractBasePostgresMapstore<UUID, EntitySet> {
    public static final String ENTITY_TYPE_ID_INDEX    = "entityTypeId";
    public static final String ID_INDEX                = "id";
    public static final String LINKED_ENTITY_SET_INDEX = "linkedEntitySets[any]";
    public static final String ORGANIZATION_INDEX      = "organizationId";
    public static final String FLAGS_INDEX             = "flags[any]";

    public EntitySetMapstore( HikariDataSource hds ) {
        super( HazelcastMap.ENTITY_SETS.name(), ENTITY_SETS, hds );
    }

    @Override protected void bind( PreparedStatement ps, UUID key, EntitySet value ) throws SQLException {
        Array contacts = PostgresArrays.createTextArray( ps.getConnection(), value.getContacts().stream() );
        Array linkedEntitySets = PostgresArrays
                .createUuidArray( ps.getConnection(), value.getLinkedEntitySets().stream() );
        Array flags = PostgresArrays
                .createTextArray( ps.getConnection(), value.getFlags().stream().map( EntitySetFlag::toString ) );
        Array partitions = PostgresArrays.createIntArray( ps.getConnection(), value.getPartitions() );

        bind( ps, key, 1 );
        ps.setString( 2, value.getName() );
        ps.setObject( 3, value.getEntityTypeId() );
        ps.setString( 4, value.getTitle() );
        ps.setString( 5, value.getDescription() );
        ps.setArray( 6, contacts );
        ps.setArray( 7, linkedEntitySets );
        ps.setObject( 8, value.getOrganizationId() );
        ps.setArray( 9, flags );
        ps.setArray( 10, partitions );
        ps.setInt( 11, value.getPartitionsVersion() );
        if ( value.getExpiration() == null ) {
            ps.setNull( 12, Types.NULL );
            ps.setNull( 13, Types.NULL );
            ps.setNull( 14, Types.NULL );
        } else {
            ps.setObject( 12, value.getExpiration().getTimeToExpiration() );
            ps.setString( 13, value.getExpiration().getExpirationBase().toString() );
            if ( value.getExpiration().getStartDateProperty().isPresent() ) {
                ps.setObject( 14, value.getExpiration().getStartDateProperty().get() );
            } else {
                ps.setNull( 14, Types.NULL );
            }
        }

        // UPDATE
        ps.setString( 15, value.getName() );
        ps.setObject( 16, value.getEntityTypeId() );
        ps.setString( 17, value.getTitle() );
        ps.setString( 18, value.getDescription() );
        ps.setArray( 19, contacts );
        ps.setArray( 20, linkedEntitySets );
        ps.setObject( 21, value.getOrganizationId() );
        ps.setArray( 22, flags );
        ps.setArray( 23, partitions );
        ps.setInt( 24, value.getPartitionsVersion() );
        if ( value.getExpiration() == null ) {
            ps.setNull( 25, Types.NULL );
            ps.setNull( 26, Types.NULL );
            ps.setNull( 27, Types.NULL );
        } else {
            ps.setObject( 25, value.getExpiration().getTimeToExpiration() );
            ps.setString( 26, value.getExpiration().getExpirationBase().toString() );
            if ( value.getExpiration().getStartDateProperty().isPresent() ) {
                ps.setObject( 27, value.getExpiration().getStartDateProperty().get() );
            } else {
                ps.setNull( 27, Types.NULL );
            }
        }
    }

    @Override protected int bind( PreparedStatement ps, UUID key, int parameterIndex ) throws SQLException {
        ps.setObject( parameterIndex++, key );
        return parameterIndex;
    }

    @Override protected EntitySet mapToValue( ResultSet rs ) throws SQLException {
        return ResultSetAdapters.entitySet( rs );
    }

    @Override protected UUID mapToKey( ResultSet rs ) throws SQLException {
        return ResultSetAdapters.id( rs );
    }

    @Override public UUID generateTestKey() {
        return UUID.randomUUID();
    }

    @Override public EntitySet generateTestValue() {
        return TestDataFactory.entitySet();
    }

    @Override public MapConfig getMapConfig() {
        return super
                .getMapConfig()
                .setInMemoryFormat( InMemoryFormat.OBJECT )
                .addMapIndexConfig( new MapIndexConfig( ENTITY_TYPE_ID_INDEX, false ) )
                .addMapIndexConfig( new MapIndexConfig( ID_INDEX, false ) )
                .addMapIndexConfig( new MapIndexConfig( LINKED_ENTITY_SET_INDEX, false ) )
                .addMapIndexConfig( new MapIndexConfig( ORGANIZATION_INDEX, false ) )
                .addMapIndexConfig( new MapIndexConfig( FLAGS_INDEX, false ) );
    }
}
