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
import com.google.common.collect.ImmutableMap;
import com.openlattice.data.PropertyMetadata;
import com.openlattice.postgres.DataTables;
import com.openlattice.postgres.PostgresArrays;
import com.openlattice.postgres.PostgresColumnDefinition;
import com.openlattice.postgres.PostgresTableDefinition;
import com.openlattice.postgres.ResultSetAdapters;
import com.openlattice.postgres.mapstores.AbstractBaseSplitKeyPostgresMapstore;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.lang.RandomStringUtils;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class PropertyDataMapstore extends AbstractBaseSplitKeyPostgresMapstore<UUID, Object, PropertyMetadata> {
    private final PostgresColumnDefinition valueColumn;

    public PropertyDataMapstore( PostgresTableDefinition table, HikariDataSource hds ) {
        //Table name doesn't matter as these aer used for configuring maps.
        super( "pdms", table, hds );
        for ( PostgresColumnDefinition pcd : table.getColumns() ) {
            if ( pcd.getName().equals( DataTables.VALUE_FIELD ) ) {
                valueColumn = pcd;
                return;
            }
        }
        throw new IllegalStateException( "Value column was not assigned." );
    }

    @Override protected List<PostgresColumnDefinition> initKeyColumns() {
        return ImmutableList.of( ID_VALUE );
    }

    @Override protected Optional<String> buildOnConflictQuery() {
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

    @Override protected void bind( PreparedStatement ps, UUID key, Object subKey, PropertyMetadata value )
            throws SQLException {
        int parameterIndex = bind( ps, key, 1 );
        ps.setObject( parameterIndex++, subKey );

        ps.setLong( parameterIndex++, value.getVersion() );
        ps.setArray( parameterIndex++, PostgresArrays.createLongArray( ps.getConnection(), value.getVersions() ) );
        ps.setObject( parameterIndex++, value.getLastWrite() );

        //Update Query parameters
        ps.setLong( parameterIndex++, value.getVersion() );
        ps.setArray( parameterIndex++, PostgresArrays.createLongArray( ps.getConnection(), value.getVersions() ) );
        ps.setObject( parameterIndex++, value.getLastWrite() );
    }

    @Override protected int bind( PreparedStatement ps, UUID key, int offset ) throws SQLException {
        ps.setObject( offset++, key );
        return offset;
    }

    @Override protected Map<Object, PropertyMetadata> mapToValue( ResultSet rs ) throws SQLException {
        final Map<Object, PropertyMetadata> value = new HashMap<>();

        if ( !rs.next() ) {
            return null;
        }

        do {
            Object key = ResultSetAdapters.propertyValue( rs );
            PropertyMetadata metadata = ResultSetAdapters.propertyMetadata( rs );
            value.put( key, metadata );
        } while ( rs.next() );

        return value;
    }

    @Override protected UUID mapToKey( ResultSet rs ) throws SQLException {
        return ResultSetAdapters.id( rs );
    }

    @Override public UUID generateTestKey() {
        return UUID.randomUUID();
    }

    @Override public Map<Object, PropertyMetadata> generateTestValue() {
        return ImmutableMap
                .of( RandomStringUtils.randomAlphanumeric( 10 ), new PropertyMetadata( 5, ImmutableList.of( 1L, 2L ),
                        OffsetDateTime.now() ) );
    }
}
