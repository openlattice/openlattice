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
        int parameterIndex = bind(ps,key, 1);

        FullQualifiedName fqn = value.getType();
        ps.setString( parameterIndex++, fqn.getNamespace() );
        ps.setString( parameterIndex++, fqn.getName() );

        ps.setString( parameterIndex++, value.getTitle() );
        ps.setString( parameterIndex++, value.getDescription() );

        Array members = PostgresArrays.createTextArray( ps.getConnection(), value.getMembers().stream() );
        ps.setArray( parameterIndex++, members );

        Array schemas = PostgresArrays.createTextArray(
                ps.getConnection(),
                value.getSchemas().stream().map( FullQualifiedName::getFullQualifiedNameAsString ) );
        ps.setArray( parameterIndex++, schemas );

        ps.setString( parameterIndex++, value.getDatatype().name() );
        ps.setBoolean( parameterIndex++, value.isFlags() );
        ps.setBoolean( parameterIndex++, value.isPIIfield() );
        ps.setString( parameterIndex++, value.getAnalyzer().name() );
        ps.setBoolean( parameterIndex++, value.isMultiValued() );
        ps.setString( parameterIndex++, value.getPostgresIndexType().name() );

        // UPDATE
        ps.setString( parameterIndex++, fqn.getNamespace() );
        ps.setString( parameterIndex++, fqn.getName() );
        ps.setString( parameterIndex++, value.getTitle() );
        ps.setString( parameterIndex++, value.getDescription() );
        ps.setArray( parameterIndex++, members );
        ps.setArray( parameterIndex++, schemas );
        ps.setString( parameterIndex++, value.getDatatype().name() );
        ps.setBoolean( parameterIndex++, value.isFlags() );
        ps.setBoolean( parameterIndex++, value.isPIIfield() );
        ps.setString( parameterIndex++, value.getAnalyzer().name() );
        ps.setBoolean( parameterIndex++, value.isMultiValued() );
        ps.setString( parameterIndex++, value.getPostgresIndexType().name() );
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
