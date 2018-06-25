package com.openlattice.postgres.mapstores;

import static com.openlattice.postgres.PostgresTable.ENTITY_TYPES;

import com.openlattice.edm.type.EntityType;
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
import org.apache.olingo.commons.api.edm.FullQualifiedName;

public class EntityTypeMapstore extends AbstractBasePostgresMapstore<UUID, EntityType> {

    public EntityTypeMapstore( HikariDataSource hds ) {
        super( HazelcastMap.ENTITY_TYPES.name(), ENTITY_TYPES, hds );
    }

    @Override protected void bind( PreparedStatement ps, UUID key, EntityType value ) throws SQLException {
        bind( ps, key, 1 );

        FullQualifiedName fqn = value.getType();
        ps.setString( 2, fqn.getNamespace() );
        ps.setString( 3, fqn.getName() );

        ps.setString( 4, value.getTitle() );
        ps.setString( 5, value.getDescription() );

        Array primaryKey = PostgresArrays.createUuidArray( ps.getConnection(), value.getKey().stream() );
        ps.setArray( 6, primaryKey );

        Array properties = PostgresArrays.createUuidArray( ps.getConnection(), value.getProperties().stream() );
        ps.setArray( 7, properties );

        ps.setObject( 8, value.getBaseType().orElse( null ) );

        Array schemas = PostgresArrays.createTextArray(
                ps.getConnection(),
                value.getSchemas().stream().map( FullQualifiedName::getFullQualifiedNameAsString ) );
        ps.setArray( 9, schemas );

        ps.setString( 10, value.getCategory().name() );

        // UPDATE
        ps.setString( 11, fqn.getNamespace() );
        ps.setString( 12, fqn.getName() );
        ps.setString( 13, value.getTitle() );
        ps.setString( 14, value.getDescription() );
        ps.setArray( 15, primaryKey );
        ps.setArray( 16, properties );
        ps.setObject( 17, value.getBaseType().orElse( null ) );
        ps.setArray( 18, schemas );
        ps.setString( 19, value.getCategory().name() );
    }

    @Override protected int bind( PreparedStatement ps, UUID key, int parameterIndex ) throws SQLException {
        ps.setObject( parameterIndex++, key );
        return parameterIndex;
    }

    @Override protected EntityType mapToValue( ResultSet rs ) throws SQLException {
        return ResultSetAdapters.entityType( rs );
    }

    @Override protected UUID mapToKey( ResultSet rs ) throws SQLException {
        return ResultSetAdapters.id( rs );
    }

    @Override public UUID generateTestKey() {
        return UUID.randomUUID();
    }

    @Override public EntityType generateTestValue() {
        return TestDataFactory.entityType();
    }
}
