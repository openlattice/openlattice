/*
 * Copyright (C) 2019. OpenLattice, Inc.
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
 *
 */

package com.openlattice.postgres.mapstores;

import static com.openlattice.postgres.PostgresTable.ORGANIZATION_ASSEMBLIES;

import com.google.common.collect.ImmutableSet;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapIndexConfig;
import com.openlattice.assembler.OrganizationAssembly;
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
import org.apache.commons.lang3.RandomStringUtils;

public class OrganizationAssemblyMapstore extends AbstractBasePostgresMapstore<UUID, OrganizationAssembly> {
    public static final String INITIALIZED_INDEX = "initialized";
    private final       UUID   testKey           = UUID.randomUUID();

    public OrganizationAssemblyMapstore( HikariDataSource hds ) {
        super( HazelcastMap.ASSEMBLIES.name(), ORGANIZATION_ASSEMBLIES, hds );
    }

    @Override protected void bind( PreparedStatement ps, UUID key, OrganizationAssembly value ) throws SQLException {
        Array entitySetIds = PostgresArrays.createUuidArray( ps.getConnection(), value.getEntitySetIds() );

        bind( ps, key, 1 );
        ps.setString( 2, value.getDbname() );
        ps.setArray( 3, entitySetIds );
        ps.setBoolean( 4, value.getInitialized() );

        // UPDATE
        ps.setString( 5, value.getDbname() );
        ps.setArray( 6, entitySetIds );
        ps.setBoolean( 7, value.getInitialized() );
    }

    @Override protected int bind( PreparedStatement ps, UUID key, int parameterIndex ) throws SQLException {
        ps.setObject( parameterIndex++, key );
        return parameterIndex;
    }

    @Override protected OrganizationAssembly mapToValue( ResultSet rs ) throws SQLException {
        return ResultSetAdapters.organizationAssembly( rs );
    }

    @Override protected UUID mapToKey( ResultSet rs ) throws SQLException {
        return ResultSetAdapters.organizationId( rs );
    }

    @Override public UUID generateTestKey() {
        return testKey;
    }

    @Override public OrganizationAssembly generateTestValue() {
        return new OrganizationAssembly(
                testKey,
                RandomStringUtils.randomAlphanumeric( 10 ),
                ImmutableSet.of( UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID() ),
                false );
    }

    @Override public MapConfig getMapConfig() {
        return super
                .getMapConfig()
                .addMapIndexConfig( new MapIndexConfig( INITIALIZED_INDEX, false ) )
                .setInMemoryFormat( InMemoryFormat.OBJECT );
    }
}
