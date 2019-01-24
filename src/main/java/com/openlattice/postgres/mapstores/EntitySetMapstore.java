package com.openlattice.postgres.mapstores;

import static com.openlattice.postgres.PostgresTable.ENTITY_SETS;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapIndexConfig;
import com.openlattice.edm.EntitySet;
import com.openlattice.hazelcast.HazelcastMap;
import com.openlattice.mapstores.TestDataFactory;
import com.openlattice.postgres.PostgresArrays;
import com.openlattice.postgres.ResultSetAdapters;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;

public class EntitySetMapstore extends AbstractBasePostgresMapstore<UUID, EntitySet> {

    public static final String LINKED_ENTITY_SET_INDEX = "linkedEntitySets[any]";

    public EntitySetMapstore( HikariDataSource hds ) {
        super( HazelcastMap.ENTITY_SETS.name(), ENTITY_SETS, hds );
    }

    @Override protected void bind( PreparedStatement ps, UUID key, EntitySet value ) throws SQLException {
        Array contacts = PostgresArrays.createTextArray( ps.getConnection(), value.getContacts().stream() );
        Array linkedEntitySets = PostgresArrays.createUuidArray( ps.getConnection(), value.getLinkedEntitySets().stream() );

        bind( ps, key, 1 );
        ps.setString( 2, value.getName() );
        ps.setObject( 3, value.getEntityTypeId() );
        ps.setString( 4, value.getTitle() );
        ps.setString( 5, value.getDescription() );
        ps.setArray( 6, contacts );
        ps.setBoolean( 7, value.isLinking() );
        ps.setArray( 8, linkedEntitySets );
        ps.setBoolean( 9, value.isExternal() );

        // UPDATE
        ps.setString( 10, value.getName() );
        ps.setObject( 11, value.getEntityTypeId() );
        ps.setString( 12, value.getTitle() );
        ps.setString( 13, value.getDescription() );
        ps.setArray( 14, contacts );
        ps.setBoolean( 15, value.isLinking() );
        ps.setArray( 16, linkedEntitySets );
        ps.setBoolean( 17, value.isExternal() );
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
                .addMapIndexConfig( new MapIndexConfig( LINKED_ENTITY_SET_INDEX, false ) );
    }
}
