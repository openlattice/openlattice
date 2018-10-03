package com.openlattice.postgres.mapstores;

import static com.openlattice.postgres.PostgresTable.COMPLEX_TYPES;

import com.dataloom.mappers.ObjectMappers;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.openlattice.edm.type.ComplexType;
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

public class ComplexTypeMapstore extends AbstractBasePostgresMapstore<UUID, ComplexType> {

    private final ObjectMapper mapper;

    public ComplexTypeMapstore( HikariDataSource hds ) {
        this( hds, ObjectMappers.newJsonMapper());
    }
    public ComplexTypeMapstore( HikariDataSource hds, ObjectMapper mapper ) {
        super( HazelcastMap.COMPLEX_TYPES.name(), COMPLEX_TYPES, hds );
        this.mapper = mapper;
    }

    @Override protected void bind( PreparedStatement ps, UUID key, ComplexType value ) throws SQLException {
        var parameterIndex = bind( ps, key, 1 );

        final var propertyTags = new PGobject();
        try {
            propertyTags.setType( "jsonb" );
            propertyTags.setValue( mapper.writeValueAsString( value.getPropertyTags() ) );
        } catch ( JsonProcessingException e ) {
            throw new SQLException( "Unable to serialize property tags to JSON.", e );
        }

        FullQualifiedName fqn = value.getType();
        ps.setString( parameterIndex++ , fqn.getNamespace() );
        ps.setString( parameterIndex++ , fqn.getName() );

        ps.setString( parameterIndex++ , value.getTitle() );
        ps.setString( parameterIndex++ , value.getDescription() );

        Array properties = PostgresArrays.createUuidArray( ps.getConnection(), value.getProperties().stream() );
        ps.setArray( parameterIndex++ , properties );

        ps.setObject( parameterIndex++, propertyTags  );
        ps.setObject( parameterIndex++ , value.getBaseType().orElse( null ) );

        Array schemas = PostgresArrays.createTextArray(
                ps.getConnection(),
                value.getSchemas().stream().map( FullQualifiedName::getFullQualifiedNameAsString ) );

        ps.setArray( parameterIndex++ , schemas );
        ps.setString( parameterIndex++ , value.getCategory().name() );

        // UPDATE
        ps.setString( parameterIndex++ , fqn.getNamespace() );
        ps.setString( parameterIndex++ , fqn.getName() );
        ps.setString( parameterIndex++ , value.getTitle() );
        ps.setString( parameterIndex++ , value.getDescription() );
        ps.setArray( parameterIndex++ , properties );
        ps.setObject( parameterIndex++, propertyTags );
        ps.setObject( parameterIndex++ , value.getBaseType().orElse( null ) );
        ps.setArray( parameterIndex++ , schemas );
        ps.setString( parameterIndex++ , value.getCategory().name() );
    }

    @Override protected int bind( PreparedStatement ps, UUID key, int parameterIndex ) throws SQLException {
        ps.setObject( parameterIndex++, key );
        return parameterIndex;
    }

    @Override protected ComplexType mapToValue( ResultSet rs ) throws SQLException {
        return ResultSetAdapters.complexType( rs );
    }

    @Override protected UUID mapToKey( ResultSet rs ) throws SQLException {
        return ResultSetAdapters.id( rs );
    }

    @Override public UUID generateTestKey() {
        return UUID.randomUUID();
    }

    @Override public ComplexType generateTestValue() {
        return TestDataFactory.complexType();
    }
}
