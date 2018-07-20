/*
 * Copyright (C) 2017. OpenLattice, Inc
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@openlattice.com
 *
 */

package com.openlattice.postgres.mapstores;

import com.openlattice.edm.type.PropertyType;
import com.openlattice.hazelcast.HazelcastMap;
import com.openlattice.mapstores.TestDataFactory;
import com.openlattice.postgres.PostgresArrays;
import com.openlattice.postgres.PostgresTable;
import com.openlattice.postgres.ResultSetAdapters;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.UUID;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

public class PropertyTypeMapstore extends AbstractBasePostgresMapstore<UUID, PropertyType> {

    public PropertyTypeMapstore( HikariDataSource hds ) {
        super( HazelcastMap.PROPERTY_TYPES.name(), PostgresTable.PROPERTY_TYPES, hds );
    }

    @Override protected void bind( PreparedStatement ps, UUID key, PropertyType value ) throws SQLException {
        bind( ps, key, 1 );
        FullQualifiedName fqn = value.getType();
        ps.setString( 2, fqn.getNamespace() );
        ps.setString( 3, fqn.getName() );

        ps.setString( 4, value.getDatatype().name() );
        ps.setString( 5, value.getTitle() );
        ps.setString( 6, value.getDescription() );

        Array schemas = PostgresArrays.createTextArray(
                ps.getConnection(),
                value.getSchemas().stream().map( FullQualifiedName::getFullQualifiedNameAsString ) );

        ps.setArray( 7, schemas );
        ps.setBoolean( 8, value.isPIIfield() );
        ps.setString( 9, value.getAnalyzer().name() );

        //UPDATE
        ps.setString( 10, fqn.getNamespace() );
        ps.setString( 11, fqn.getName() );

        ps.setString( 12, value.getDatatype().name() );
        ps.setString( 13, value.getTitle() );
        ps.setString( 14, value.getDescription() );

        ps.setArray( 15, schemas );
        ps.setBoolean( 16, value.isPIIfield() );
        ps.setString( 17, value.getAnalyzer().name() );
        ps.setBoolean( 18, value.getAnalyzer().name() );
    }

    @Override protected int bind( PreparedStatement ps, UUID key, int parameterIndex ) throws SQLException {
        ps.setObject( parameterIndex++, key );
        return parameterIndex;
    }

    @Override protected PropertyType mapToValue( java.sql.ResultSet rs ) throws SQLException {
        return ResultSetAdapters.propertyType( rs );
    }

    @Override protected UUID mapToKey( java.sql.ResultSet rs ) throws SQLException {
        return ResultSetAdapters.id( rs );
    }

    @Override
    public UUID generateTestKey() {
        return UUID.randomUUID();
    }

    @Override
    public PropertyType generateTestValue() {
        return TestDataFactory.propertyType();
    }
}
