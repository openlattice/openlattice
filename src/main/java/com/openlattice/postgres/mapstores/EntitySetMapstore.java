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
import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;

public class EntitySetMapstore extends AbstractBasePostgresMapstore<UUID, EntitySet> {
    public static final String ENTITY_TYPE_ID_INDEX    = "entityTypeId";
    public static final String ID_INDEX                = "id";
    public static final String LINKED_ENTITY_SET_INDEX = "linkedEntitySets[any]";
    public static final String ORGANIZATION_INDEX      = "organizationId";

    public EntitySetMapstore( HikariDataSource hds ) {
        super( HazelcastMap.ENTITY_SETS.name(), ENTITY_SETS, hds );
    }

    @Override protected void bind( PreparedStatement ps, UUID key, EntitySet value ) throws SQLException {
        Array contacts = PostgresArrays.createTextArray( ps.getConnection(), value.getContacts().stream() );
        Array linkedEntitySets = PostgresArrays
                .createUuidArray( ps.getConnection(), value.getLinkedEntitySets().stream() );
        Array flags = PostgresArrays
                .createTextArray( ps.getConnection(), value.getFlags().stream().map( EntitySetFlag::toString ) );
        Array partitions = PostgresArrays.createIntArray(ps.getConnection(), value.getPartitions()  );

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

        // UPDATE
        ps.setString( 12, value.getName() );
        ps.setObject( 13, value.getEntityTypeId() );
        ps.setString( 14, value.getTitle() );
        ps.setString( 15, value.getDescription() );
        ps.setArray( 16, contacts );
        ps.setArray( 17, linkedEntitySets );
        ps.setObject( 18, value.getOrganizationId() );
        ps.setArray( 19, flags );
        ps.setArray( 20, partitions );
        ps.setInt( 21, value.getPartitionsVersion() );
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
                .addMapIndexConfig( new MapIndexConfig( ORGANIZATION_INDEX, false ) );
    }
}
