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

import com.openlattice.hazelcast.HazelcastMap;
import com.google.common.collect.ImmutableSet;
import com.openlattice.postgres.PostgresArrays;
import com.openlattice.postgres.PostgresTable;
import com.openlattice.postgres.ResultSetAdapters;
import com.openlattice.rhizome.hazelcast.DelegatedUUIDSet;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class LinkedEntityTypesMapstore extends AbstractBasePostgresMapstore<UUID, DelegatedUUIDSet> {
    public LinkedEntityTypesMapstore( HikariDataSource hds ) {
        super( HazelcastMap.LINKED_ENTITY_TYPES.name(), PostgresTable.LINKED_ENTITY_TYPES, hds );
    }

    @Override public UUID generateTestKey() {
        return UUID.randomUUID();
    }

    @Override public DelegatedUUIDSet generateTestValue() {
        return new DelegatedUUIDSet( ImmutableSet.of( UUID.randomUUID(), UUID.randomUUID() ) );
    }

    @Override protected void bind( PreparedStatement ps, UUID key, DelegatedUUIDSet value ) throws SQLException {
        bind( ps, key , 1 );

        ps.setObject( 2, PostgresArrays.createUuidArray( ps.getConnection(), value ) );

        ps.setObject( 3, PostgresArrays.createUuidArray( ps.getConnection(), value ) );
    }

    @Override protected int bind( PreparedStatement ps, UUID key , int parameterIndex ) throws SQLException {
        ps.setObject( parameterIndex++, key );
        return parameterIndex;
    }

    @Override protected DelegatedUUIDSet mapToValue( ResultSet rs ) throws SQLException {
        return new DelegatedUUIDSet( ResultSetAdapters.entityTypeIds( rs ) );
    }

    @Override protected UUID mapToKey( ResultSet rs ) throws SQLException {
        return ResultSetAdapters.id( rs );
    }
}
