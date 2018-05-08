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
import static com.openlattice.postgres.PostgresColumn.ENTITY_SET_ID;
import static com.openlattice.postgres.PostgresColumn.HASH;
import static com.openlattice.postgres.PostgresColumn.ID_VALUE;
import static com.openlattice.postgres.PostgresColumn.VERSION;
import static com.openlattice.postgres.PostgresColumn.VERSIONS;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.openlattice.data.EntityDataKey;
import com.openlattice.data.PropertyMetadata;
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
import java.util.stream.Stream;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class PropertyDataMapstore
        extends AbstractBaseSplitKeyPostgresMapstore<EntityDataKey, Object, PropertyMetadata> {
    private static final Map<String, PostgresColumnDefinition> valueColumns = Maps.newConcurrentMap();
    private final String valueColumnName;

    public PropertyDataMapstore(
            PostgresColumnDefinition valueColumn,
            PostgresTableDefinition table,
            HikariDataSource hds ) {
        //Table name doesn't matter as these are used for configuring maps.
        super( "pdms", table, hds, valueColumns.putIfAbsent( table.getName(), valueColumn ) );
        valueColumnName = valueColumn.getName().replace( "\"", "" );
    }

    @Override protected List<PostgresColumnDefinition> initKeyColumns() {
        return ImmutableList.of( ENTITY_SET_ID, ID_VALUE );
    }

    @Override protected String buildInsertQuery() {
        return super.buildInsertQuery();
    }

    @Override protected Optional<String> buildOnConflictQuery() {
        return Optional.of( ( " ON CONFLICT ("
                + Stream
                .of( ENTITY_SET_ID.getName(), ID_VALUE.getName(), HASH.getName() )
                .collect( Collectors.joining( ", " ) )
                + ") DO "
                + table.updateQuery( ImmutableList.of( ID_VALUE, HASH ),
                ImmutableList.of( valCol(), VERSION, VERSIONS, LAST_WRITE ),
                false ) ) );
    }

    private PostgresColumnDefinition valCol() {
        return valueColumns.get( getTable() );
    }

    @Override protected List<PostgresColumnDefinition> initValueColumns() {
        return ImmutableList.of( HASH, valCol(), VERSION, VERSIONS, LAST_WRITE );
    }

    @Override protected void bind( PreparedStatement ps, EntityDataKey key, Object subKey, PropertyMetadata value )
            throws SQLException {
        int parameterIndex = bind( ps, key, 1 );
        ps.setBytes( parameterIndex++, value.getHash() );

        ps.setObject( parameterIndex++, subKey );
        ps.setLong( parameterIndex++, value.getVersion() );
        ps.setArray( parameterIndex++, PostgresArrays.createLongArray( ps.getConnection(), value.getVersions() ) );
        ps.setObject( parameterIndex++, value.getLastWrite() );

        //Update Query parameters
        ps.setObject( parameterIndex++, subKey );
        ps.setLong( parameterIndex++, value.getVersion() );
        ps.setArray( parameterIndex++, PostgresArrays.createLongArray( ps.getConnection(), value.getVersions() ) );
        ps.setObject( parameterIndex++, value.getLastWrite() );
    }

    @Override protected int bind( PreparedStatement ps, EntityDataKey key, int offset ) throws SQLException {
        ps.setObject( offset++, key.getEntitySetId() );
        ps.setObject( offset++, key.getEntityKeyId() );
        return offset;
    }

    @Override protected Map<Object, PropertyMetadata> mapToValue( ResultSet rs ) throws SQLException {
        final Map<Object, PropertyMetadata> value = new HashMap<>();

        do {
            Object key = ResultSetAdapters.propertyValue( valueColumnName, rs );
            PropertyMetadata metadata = ResultSetAdapters.propertyMetadata( rs );
            value.put( key, metadata );
        } while ( rs.next() );

        return value;
    }

    @Override protected EntityDataKey mapToKey( ResultSet rs ) throws SQLException {
        return ResultSetAdapters.entityDataKey( rs );
    }

    @Override public EntityDataKey generateTestKey() {
        return new EntityDataKey( UUID.randomUUID(), UUID.randomUUID() );
    }

    @Override public Map<Object, PropertyMetadata> generateTestValue() {
        return ImmutableMap
                .of( RandomStringUtils.randomAlphanumeric( 10 ),
                        new PropertyMetadata( RandomUtils.nextBytes( 16 ), 5, ImmutableList.of( 1L, 2L ),
                                OffsetDateTime.now() ) );
    }
}
