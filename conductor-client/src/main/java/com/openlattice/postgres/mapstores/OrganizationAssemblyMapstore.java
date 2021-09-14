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

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.config.MapConfig;
import com.openlattice.assembler.OrganizationAssembly;
import com.openlattice.hazelcast.HazelcastMap;
import com.openlattice.organization.OrganizationEntitySetFlag;
import com.openlattice.postgres.ResultSetAdapters;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.EnumSet;
import java.util.Map;
import java.util.UUID;

public class OrganizationAssemblyMapstore extends AbstractBasePostgresMapstore<UUID, OrganizationAssembly> {
    public static final String INITIALIZED_INDEX                 = "initialized";
    public static final String MATERIALIZED_ENTITY_SETS_ID_INDEX = "materializedEntitySets.keySet[any]";
    private final       UUID   testKey                           = UUID.randomUUID();

    private final MaterializedEntitySetMapStore materializedEntitySetsMapStore;

    public OrganizationAssemblyMapstore( HikariDataSource hds ) {
        super( HazelcastMap.ASSEMBLIES, ORGANIZATION_ASSEMBLIES, hds );
        materializedEntitySetsMapStore = new MaterializedEntitySetMapStore( hds );
    }

    @Override protected void bind( PreparedStatement ps, UUID key, OrganizationAssembly value ) throws SQLException {

        bind( ps, key, 1 );
        ps.setBoolean( 2, value.getInitialized() );

        // UPDATE
        ps.setBoolean( 3, value.getInitialized() );
    }

    @Override protected int bind( PreparedStatement ps, UUID key, int parameterIndex ) throws SQLException {
        ps.setObject( parameterIndex++, key );
        return parameterIndex;
    }

    @Override protected OrganizationAssembly mapToValue( ResultSet rs ) throws SQLException {
        final UUID organizationId = ResultSetAdapters.organizationId( rs );
        final boolean initialized = ResultSetAdapters.initialized( rs );

        final Map<UUID, EnumSet<OrganizationEntitySetFlag>> materializedEntitySets =
                materializedEntitySetsMapStore
                        .loadMaterializedEntitySetsForOrganization( rs.getStatement().getConnection(), organizationId );

        return new OrganizationAssembly( organizationId, initialized, materializedEntitySets );
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
                false,
                Map.of() );
    }

    @Override public MapConfig getMapConfig() {
        return super
                .getMapConfig()
                .addIndexConfig( new IndexConfig( IndexType.HASH, INITIALIZED_INDEX ) )
                .addIndexConfig( new IndexConfig( IndexType.HASH, MATERIALIZED_ENTITY_SETS_ID_INDEX ) )
                .setInMemoryFormat( InMemoryFormat.OBJECT );
    }
}
