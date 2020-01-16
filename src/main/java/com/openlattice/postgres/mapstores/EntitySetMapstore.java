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
        super( HazelcastMap.ENTITY_SETS, ENTITY_SETS, hds );
    }

    @Override protected void bind( PreparedStatement ps, UUID key, EntitySet value ) throws SQLException {
        Array contacts = PostgresArrays.createTextArray( ps.getConnection(), value.getContacts().stream() );
        Array linkedEntitySets = PostgresArrays
                .createUuidArray( ps.getConnection(), value.getLinkedEntitySets().stream() );
        Array flags = PostgresArrays
                .createTextArray( ps.getConnection(), value.getFlags().stream().map( EntitySetFlag::toString ) );
        Array partitions = PostgresArrays.createIntArray( ps.getConnection(), value.getPartitions() );

        int index = bind( ps, key, 1 );
        ps.setString( index++, value.getName() );
        ps.setObject( index++, value.getEntityTypeId() );
        ps.setString( index++, value.getTitle() );
        ps.setString( index++, value.getDescription() );
        ps.setArray( index++, contacts );
        ps.setArray( index++, linkedEntitySets );
        ps.setObject( index++, value.getOrganizationId() );
        ps.setArray( index++, flags );
        ps.setArray( index++, partitions );
        if ( value.getExpiration() == null ) {
            ps.setNull( index++, Types.NULL );
            ps.setNull( index++, Types.NULL );
            ps.setNull( index++, Types.NULL );
            ps.setNull( index++, Types.NULL );
        } else {
            ps.setLong( index++, value.getExpiration().getTimeToExpiration() );
            ps.setString( index++, value.getExpiration().getExpirationBase().toString() );
            ps.setString( index++, value.getExpiration().getDeleteType().toString() );
            if ( value.getExpiration().getStartDateProperty().isPresent() ) {
                ps.setObject( index++, value.getExpiration().getStartDateProperty().get() );
            } else {
                ps.setNull( index++, Types.NULL );
            }
        }

        // UPDATE
        ps.setString( index++, value.getName() );
        ps.setObject( index++, value.getEntityTypeId() );
        ps.setString( index++, value.getTitle() );
        ps.setString( index++, value.getDescription() );
        ps.setArray( index++, contacts );
        ps.setArray( index++, linkedEntitySets );
        ps.setObject( index++, value.getOrganizationId() );
        ps.setArray( index++, flags );
        ps.setArray( index++, partitions );
        if ( value.getExpiration() == null ) {
            ps.setNull( index++, Types.NULL );
            ps.setNull( index++, Types.NULL );
            ps.setNull( index++, Types.NULL );
            ps.setNull( index++, Types.NULL );
        } else {
            ps.setLong( index++, value.getExpiration().getTimeToExpiration() );
            ps.setString( index++, value.getExpiration().getExpirationBase().toString() );
            ps.setString( index++, value.getExpiration().getDeleteType().toString() );
            if ( value.getExpiration().getStartDateProperty().isPresent() ) {
                ps.setObject( index++, value.getExpiration().getStartDateProperty().get() );
            } else {
                ps.setNull( index++, Types.NULL );
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
