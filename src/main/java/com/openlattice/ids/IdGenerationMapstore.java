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

package com.openlattice.ids;

import static com.openlattice.postgres.PostgresColumn.PARTITION_INDEX_FIELD;

import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.MapStoreConfig.InitialLoadMode;
import com.openlattice.hazelcast.HazelcastMap;
import com.openlattice.postgres.PostgresTable;
import com.openlattice.postgres.ResultSetAdapters;
import com.openlattice.postgres.mapstores.AbstractBasePostgresMapstore;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class IdGenerationMapstore extends AbstractBasePostgresMapstore<Long, Range> {
    public IdGenerationMapstore( HikariDataSource hds ) {
        super( HazelcastMap.ID_GENERATION.name(), PostgresTable.ID_GENERATION, hds );
    }

    @Override public Long generateTestKey() {
        return 1L;
    }

    @Override public Range generateTestValue() {
        return new Range( generateTestKey() << 48L, 1, 2 );
    }

    @Override protected void bind( PreparedStatement ps, Long key, Range value ) throws SQLException {
        int parameterIndex = bind( ps, key, 1 );

        ps.setLong( parameterIndex++, value.getMsb() );
        ps.setLong( parameterIndex++, value.getLsb() );

        //Update clause
        ps.setLong( parameterIndex++, value.getMsb() );
        ps.setLong( parameterIndex++, value.getLsb() );

    }

    @Override public MapStoreConfig getMapStoreConfig() {
        return super.getMapStoreConfig().setInitialLoadMode( InitialLoadMode.EAGER );
    }

    @Override public MapConfig getMapConfig() {
        return super.getMapConfig();
    }

    @Override protected int bind( PreparedStatement ps, Long key, int parameterIndex ) throws SQLException {
        ps.setLong( parameterIndex++, key );
        return parameterIndex;
    }

    @Override protected Range mapToValue( ResultSet rs ) throws SQLException {
        return ResultSetAdapters.range( rs );
    }

    @Override protected Long mapToKey( ResultSet rs ) throws SQLException {
        return rs.getLong( PARTITION_INDEX_FIELD );
    }
}
