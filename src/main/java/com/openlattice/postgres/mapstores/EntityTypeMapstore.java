package com.openlattice.postgres.mapstores;

import static com.openlattice.postgres.PostgresTable.ENTITY_TYPES;

import com.dataloom.mappers.ObjectMappers;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import org.postgresql.util.PGobject;

public class EntityTypeMapstore extends AbstractBasePostgresMapstore<UUID, EntityType> {

    private final ObjectMapper mapper;

    public EntityTypeMapstore( HikariDataSource hds ) {
        this( hds, ObjectMappers.newJsonMapper() );
    }

    public EntityTypeMapstore( HikariDataSource hds, ObjectMapper mapper ) {
        super( HazelcastMap.ENTITY_TYPES.name(), ENTITY_TYPES, hds );
        this.mapper = mapper;
    }

    @Override protected void bind( PreparedStatement ps, UUID key, EntityType value ) throws SQLException {
        //This is pretty rarely called so efficiency is not as important. Much better to use constants for hot binds
        var parameterIndex = bind( ps, key, 1 );
        final var propertyTags = new PGobject();
        try {
            propertyTags.setType( "jsonb" );
            propertyTags.setValue( mapper.writeValueAsString( value.getPropertyTags() ) );
        } catch ( JsonProcessingException e ) {
            throw new SQLException( "Unable to serialize property tags to JSON.", e );
        }

        FullQualifiedName fqn = value.getType();
        ps.setString( parameterIndex++, fqn.getNamespace() );
        ps.setString( parameterIndex++, fqn.getName() );

        ps.setString( parameterIndex++, value.getTitle() );
        ps.setString( parameterIndex++, value.getDescription() );

        Array primaryKey = PostgresArrays.createUuidArray( ps.getConnection(), value.getKey().stream() );
        ps.setArray( parameterIndex++, primaryKey );

        Array properties = PostgresArrays.createUuidArray( ps.getConnection(), value.getProperties().stream() );
        ps.setArray( parameterIndex++, properties );

        ps.setObject( parameterIndex++, propertyTags );
        ps.setObject( parameterIndex++, value.getBaseType().orElse( null ) );

        Array schemas = PostgresArrays.createTextArray(
                ps.getConnection(),
                value.getSchemas().stream().map( FullQualifiedName::getFullQualifiedNameAsString ) );
        ps.setArray( parameterIndex++, schemas );

        ps.setString( parameterIndex++, value.getCategory().name() );
        ps.setInt( parameterIndex++, value.getShards() );

        // UPDATE
        ps.setString( parameterIndex++, fqn.getNamespace() );
        ps.setString( parameterIndex++, fqn.getName() );
        ps.setString( parameterIndex++, value.getTitle() );
        ps.setString( parameterIndex++, value.getDescription() );
        ps.setArray( parameterIndex++, primaryKey );
        ps.setArray( parameterIndex++, properties );
        ps.setObject( parameterIndex++, propertyTags );
        ps.setObject( parameterIndex++, value.getBaseType().orElse( null ) );
        ps.setArray( parameterIndex++, schemas );
        ps.setString( parameterIndex++, value.getCategory().name() );
        ps.setInt( parameterIndex++, value.getShards() );

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
