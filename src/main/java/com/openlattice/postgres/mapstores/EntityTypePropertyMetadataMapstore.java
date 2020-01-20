package com.openlattice.postgres.mapstores;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapIndexConfig;
import com.openlattice.edm.set.EntitySetPropertyKey;
import com.openlattice.edm.set.EntitySetPropertyMetadata;
import com.openlattice.edm.type.EntityTypePropertyKey;
import com.openlattice.edm.type.EntityTypePropertyMetadata;
import com.openlattice.hazelcast.HazelcastMap;
import com.openlattice.mapstores.TestDataFactory;
import com.openlattice.postgres.PostgresArrays;
import com.openlattice.postgres.ResultSetAdapters;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.UUID;

import static com.openlattice.postgres.PostgresTable.ENTITY_TYPE_PROPERTY_METADATA;

public class EntityTypePropertyMetadataMapstore
        extends AbstractBasePostgresMapstore<EntityTypePropertyKey, EntityTypePropertyMetadata> {

    public static final String ENTITY_TYPE_INDEX = "__key#entityTypeId";

    public EntityTypePropertyMetadataMapstore( HikariDataSource hds ) {
        super( HazelcastMap.ENTITY_TYPE_PROPERTY_METADATA, ENTITY_TYPE_PROPERTY_METADATA, hds );
    }

    @Override protected void bind(
            PreparedStatement ps, EntityTypePropertyKey key, EntityTypePropertyMetadata value ) throws SQLException {
        var parameterIndex = bind( ps, key, 1 );

        final var tags = PostgresArrays.createTextArray( ps.getConnection(), value.getTags() );

        ps.setString( parameterIndex++, value.getTitle() );
        ps.setString( parameterIndex++, value.getDescription() );
        ps.setArray( parameterIndex++, tags );
        ps.setBoolean( parameterIndex++, value.getDefaultShow() );

        ps.setString( parameterIndex++, value.getTitle() );
        ps.setString( parameterIndex++, value.getDescription() );
        ps.setArray( parameterIndex++, tags );
        ps.setBoolean( parameterIndex++, value.getDefaultShow() );
    }

    @Override protected int bind( PreparedStatement ps, EntityTypePropertyKey key, int parameterIndex )
            throws SQLException {
        ps.setObject( parameterIndex++, key.getEntityTypeId() );
        ps.setObject( parameterIndex++, key.getPropertyTypeId() );
        return parameterIndex;
    }

    @Override protected EntityTypePropertyMetadata mapToValue( ResultSet rs ) throws SQLException {
        return ResultSetAdapters.entityTypePropertyMetadata( rs );
    }

    @Override protected EntityTypePropertyKey mapToKey( ResultSet rs ) throws SQLException {
        return ResultSetAdapters.entityTypePropertyKey( rs );
    }

    @Override public EntityTypePropertyKey generateTestKey() {
        return new EntityTypePropertyKey( UUID.randomUUID(), UUID.randomUUID() );
    }

    @Override public EntityTypePropertyMetadata generateTestValue() {
        return new EntityTypePropertyMetadata( TestDataFactory.random( 10 ),
                TestDataFactory.random( 10 ),
                new LinkedHashSet<>( Arrays
                        .asList( TestDataFactory.random( 5 ), TestDataFactory.random( 5 ) ) ),
                true );
    }
    @Override public MapConfig getMapConfig() {
        return super
                .getMapConfig()
                .setInMemoryFormat( InMemoryFormat.OBJECT )
                .addMapIndexConfig( new MapIndexConfig( ENTITY_TYPE_INDEX, false ) );
    }
}
