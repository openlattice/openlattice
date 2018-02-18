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

import static com.openlattice.postgres.DataTables.LAST_INDEX;
import static com.openlattice.postgres.DataTables.LAST_WRITE;
import static com.openlattice.postgres.PostgresColumn.VERSION;

import com.google.common.collect.ImmutableList;
import com.openlattice.data.EntityDataMetadata;
import com.openlattice.postgres.PostgresColumnDefinition;
import com.openlattice.postgres.PostgresTableDefinition;
import com.openlattice.postgres.ResultSetAdapters;
import com.openlattice.postgres.mapstores.AbstractBasePostgresMapstore;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;

/**
 * Map from entity key id to entity data metadata.
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class EntityDataMapstore extends AbstractBasePostgresMapstore<UUID, EntityDataMetadata> {

    public EntityDataMapstore( HikariDataSource hds, PostgresTableDefinition table ) {
        super( "edms", table, hds );
    }

    @Override protected List<PostgresColumnDefinition> initValueColumns() {
        return ImmutableList.of( VERSION, LAST_WRITE, LAST_INDEX );
    }

    @Override public UUID generateTestKey() {
        return null;
    }

    @Override public EntityDataMetadata generateTestValue() {
        return null;
    }

    @Override protected void bind( PreparedStatement ps, UUID key, EntityDataMetadata value )
            throws SQLException {
        bind( ps, key, 1 );

        ps.setLong( 2, value.getVersion() );
        ps.setObject( 3, value.getLastWrite() );
        ps.setObject( 4, value.getLastIndex() );

        ps.setLong( 5, value.getVersion() );
        ps.setObject( 6, value.getLastWrite() );
        ps.setObject( 7, value.getLastIndex() );

//        ps.setObject( 8, key );
    }

    @Override
    protected int bind( PreparedStatement ps, UUID key, int offset ) throws SQLException {
        ps.setObject( offset++, key );
        return offset;
    }

    @Override
    protected EntityDataMetadata mapToValue( ResultSet rs ) throws SQLException {
        return ResultSetAdapters.entityDataMetadata( rs );
    }

    @Override
    protected UUID mapToKey( ResultSet rs ) throws SQLException {
        return ResultSetAdapters.id( rs );
    }

}
