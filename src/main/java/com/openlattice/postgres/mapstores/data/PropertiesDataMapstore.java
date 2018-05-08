/*
 * Copyright (C) 2018. OpenLattice, Inc
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

package com.openlattice.postgres.mapstores.data;

import static com.openlattice.postgres.DataTables.LAST_WRITE;
import static com.openlattice.postgres.PostgresColumn.ID_VALUE;
import static com.openlattice.postgres.PostgresColumn.VERSION;
import static com.openlattice.postgres.PostgresColumn.VERSIONS;

import com.google.common.collect.ImmutableList;
import com.openlattice.data.PropertyMetadata;
import com.openlattice.data.PropertyValueKey;
import com.openlattice.postgres.DataTables;
import com.openlattice.postgres.PostgresArrays;
import com.openlattice.postgres.PostgresColumnDefinition;
import com.openlattice.postgres.PostgresTableDefinition;
import com.openlattice.postgres.ResultSetAdapters;
import com.openlattice.postgres.mapstores.AbstractBasePostgresMapstore;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.lang3.RandomUtils;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class PropertiesDataMapstore extends AbstractBasePostgresMapstore<PropertyValueKey, PropertyMetadata> {
    private final PostgresColumnDefinition valueColumn;
    private final String                   valueColumnName;

    public PropertiesDataMapstore( PostgresTableDefinition table, HikariDataSource hds ) {
        //Table name doesn't matter as these aer used for configuring maps.
        super( "pdms", table, hds );
        for ( PostgresColumnDefinition pcd : table.getColumns() ) {
            if ( pcd.getName().equals( DataTables.VALUE_FIELD ) ) {
                valueColumn = pcd;
                valueColumnName = valueColumn.getName().replace( "\"", "" );
                return;
            }
        }
        throw new IllegalStateException( "Value column was not assigned." );
    }

    @Override protected List<PostgresColumnDefinition> initKeyColumns() {
        return ImmutableList.of( ID_VALUE );
    }

    protected Optional<String> buildOnConflictQuery() {
        return Optional.of( ( " ON CONFLICT ("
                + keyColumns().stream()
                .map( PostgresColumnDefinition::getName )
                .collect( Collectors.joining( ", " ) )
                + ") DO "
                + table.updateQuery( ImmutableList.of( ID_VALUE, valueColumn ),
                ImmutableList.of( VERSION, VERSIONS, LAST_WRITE ),
                false ) ) );
    }

    @Override protected List<PostgresColumnDefinition> initValueColumns() {
        return ImmutableList.of( valueColumn, VERSION, VERSIONS, LAST_WRITE );
        //        return ImmutableList.copyOf( Sets.difference( table.getColumns(), ImmutableSet.of( ID_VALUE ) ) );
    }

    @Override protected void bind( PreparedStatement ps, PropertyValueKey key, PropertyMetadata value )
            throws SQLException {
        int parameterIndex = bind( ps, key, 1 );

        ps.setLong( parameterIndex++, value.getVersion() );
        ps.setArray( parameterIndex++, PostgresArrays.createLongArray( ps.getConnection(), value.getVersions() ) );
        ps.setObject( parameterIndex++, value.getLastWrite() );

        //Update Query parameters
        ps.setLong( parameterIndex++, value.getVersion() );
        ps.setArray( parameterIndex++, PostgresArrays.createLongArray( ps.getConnection(), value.getVersions() ) );
        ps.setObject( parameterIndex++, value.getLastWrite() );
    }

    @Override protected int bind( PreparedStatement ps, PropertyValueKey key, int offset ) throws SQLException {
        ps.setObject( offset++, key.getEntityKeyId() );
        ps.setObject( offset++, key.getValue() );
        return offset;
    }

    @Override protected PropertyMetadata mapToValue( ResultSet rs ) throws SQLException {
        return ResultSetAdapters.propertyMetadata( rs );
    }

    @Override protected PropertyValueKey mapToKey( ResultSet rs ) throws SQLException {
        return ResultSetAdapters.propertyValueKey( valueColumnName, rs );
    }

    @Override public PropertyValueKey generateTestKey() {
        return new PropertyValueKey( UUID.randomUUID(), UUID.randomUUID().toString() );
    }

    @Override public PropertyMetadata generateTestValue() {
        return new PropertyMetadata( RandomUtils.nextBytes( 16 ), 5, ImmutableList.of( 1L, 2L ), OffsetDateTime.now() );
    }
}
