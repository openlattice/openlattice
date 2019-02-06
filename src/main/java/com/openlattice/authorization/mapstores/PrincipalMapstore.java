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

package com.openlattice.authorization.mapstores;

import static com.openlattice.postgres.PostgresTable.PRINCIPALS;

import com.openlattice.hazelcast.HazelcastMap;
import com.openlattice.mapstores.TestDataFactory;
import com.openlattice.organization.roles.Role;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapIndexConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.MapStoreConfig.InitialLoadMode;
import com.openlattice.authorization.AclKey;
import com.openlattice.authorization.SecurablePrincipal;
import com.openlattice.postgres.PostgresArrays;
import com.openlattice.postgres.ResultSetAdapters;
import com.openlattice.postgres.mapstores.AbstractBasePostgresMapstore;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class PrincipalMapstore extends AbstractBasePostgresMapstore<AclKey, SecurablePrincipal> {
    private static Role TEST_ROLE = TestDataFactory.role();

    public PrincipalMapstore( HikariDataSource hds ) {
        super( HazelcastMap.PRINCIPALS.name(), PRINCIPALS, hds );
    }

    @Override public AclKey generateTestKey() {
        return TEST_ROLE.getAclKey();
    }

    @Override public SecurablePrincipal generateTestValue() {
        return TEST_ROLE;
    }

    @Override protected void bind(
            PreparedStatement ps, AclKey key, SecurablePrincipal value ) throws SQLException {
        bind( ps, key, 1 );

        ps.setString( 2, value.getPrincipalType().name() );
        ps.setString( 3, value.getName() );
        ps.setString( 4, value.getTitle() );
        ps.setString( 5, value.getDescription() );

        ps.setString( 6, value.getPrincipalType().name() );
        ps.setString( 7, value.getName() );
        ps.setString( 8, value.getTitle() );
        ps.setString( 9, value.getDescription() );
    }

    @Override protected int bind( PreparedStatement ps, AclKey key, int parameterIndex ) throws SQLException {
        ps.setArray( parameterIndex++, PostgresArrays.createUuidArray( ps.getConnection(), key.stream() ) );
        return parameterIndex;
    }

    @Override
    protected SecurablePrincipal mapToValue( ResultSet rs ) throws SQLException {
        return ResultSetAdapters.securablePrincipal( rs );
    }

    @Override protected AclKey mapToKey( ResultSet rs ) throws SQLException {
        return ResultSetAdapters.aclKey( rs );
    }

    @Override public MapStoreConfig getMapStoreConfig() {
        return super.getMapStoreConfig().setInitialLoadMode( InitialLoadMode.EAGER );
    }

    @Override public MapConfig getMapConfig() {
        return super.getMapConfig()
                .addMapIndexConfig( new MapIndexConfig( "principal", false ) )
                .addMapIndexConfig( new MapIndexConfig( "aclKey[0]", false ) )
                .addMapIndexConfig( new MapIndexConfig( "principalType", false ) );
    }
}
