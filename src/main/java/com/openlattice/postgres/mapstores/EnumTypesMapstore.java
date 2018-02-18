package com.openlattice.postgres.mapstores;

import static com.openlattice.postgres.PostgresTable.ENUM_TYPES;

import com.openlattice.edm.type.EnumType;
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

public class EnumTypesMapstore extends AbstractBasePostgresMapstore<UUID, EnumType> {

    public EnumTypesMapstore( HikariDataSource hds ) {
        super( HazelcastMap.ENUM_TYPES.name(), ENUM_TYPES, hds );
    }

    @Override protected void bind( PreparedStatement ps, UUID key, EnumType value ) throws SQLException {
        bind(ps,key, 1);

        FullQualifiedName fqn = value.getType();
        ps.setString( 2, fqn.getNamespace() );
        ps.setString( 3, fqn.getName() );

        ps.setString( 4, value.getTitle() );
        ps.setString( 5, value.getDescription() );

        Array members = PostgresArrays.createTextArray( ps.getConnection(), value.getMembers().stream() );
        ps.setArray( 6, members );

        Array schemas = PostgresArrays.createTextArray(
                ps.getConnection(),
                value.getSchemas().stream().map( FullQualifiedName::getFullQualifiedNameAsString ) );
        ps.setArray( 7, schemas );

        ps.setString( 8, value.getDatatype().name() );
        ps.setBoolean( 9, value.isFlags() );
        ps.setBoolean( 10, value.isPIIfield() );
        ps.setString( 11, value.getAnalyzer().name() );

        // UPDATE
        ps.setString( 12, fqn.getNamespace() );
        ps.setString( 13, fqn.getName() );
        ps.setString( 14, value.getTitle() );
        ps.setString( 15, value.getDescription() );
        ps.setArray( 16, members );
        ps.setArray( 17, schemas );
        ps.setString( 18, value.getDatatype().name() );
        ps.setBoolean( 19, value.isFlags() );
        ps.setBoolean( 20, value.isPIIfield() );
        ps.setString( 21, value.getAnalyzer().name() );
    }

    @Override protected int bind( PreparedStatement ps, UUID key, int parameterIndex ) throws SQLException {
        ps.setObject( parameterIndex++, key );
        return parameterIndex;
    }

    @Override protected EnumType mapToValue( ResultSet rs ) throws SQLException {
        return ResultSetAdapters.enumType( rs );
    }

    @Override protected UUID mapToKey( ResultSet rs ) throws SQLException {
        return ResultSetAdapters.id( rs );
    }

    @Override public UUID generateTestKey() {
        return UUID.randomUUID();
    }

    @Override public EnumType generateTestValue() {
        return TestDataFactory.enumType();
    }
}
