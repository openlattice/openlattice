package com.openlattice.postgres.mapstores;

import static com.openlattice.postgres.PostgresTable.COMPLEX_TYPES;

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

public class ComplexTypeMapstore extends AbstractBasePostgresMapstore<UUID, ComplexType> {

    public ComplexTypeMapstore( HikariDataSource hds ) {
        super( HazelcastMap.COMPLEX_TYPES.name(), COMPLEX_TYPES, hds );
    }

    @Override protected void bind( PreparedStatement ps, UUID key, ComplexType value ) throws SQLException {
        bind( ps, key, 1 );

        FullQualifiedName fqn = value.getType();
        ps.setString( 2, fqn.getNamespace() );
        ps.setString( 3, fqn.getName() );

        ps.setString( 4, value.getTitle() );
        ps.setString( 5, value.getDescription() );

        Array properties = PostgresArrays.createUuidArray( ps.getConnection(), value.getProperties().stream() );
        ps.setArray( 6, properties );

        ps.setObject( 7, value.getBaseType().orElse( null ) );

        Array schemas = PostgresArrays.createTextArray(
                ps.getConnection(),
                value.getSchemas().stream().map( FullQualifiedName::getFullQualifiedNameAsString ) );

        ps.setArray( 8, schemas );
        ps.setString( 9, value.getCategory().name() );

        // UPDATE
        ps.setString( 10, fqn.getNamespace() );
        ps.setString( 11, fqn.getName() );
        ps.setString( 12, value.getTitle() );
        ps.setString( 13, value.getDescription() );
        ps.setArray( 14, properties );
        ps.setObject( 15, value.getBaseType().orElse( null ) );
        ps.setArray( 16, schemas );
        ps.setString( 17, value.getCategory().name() );
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
