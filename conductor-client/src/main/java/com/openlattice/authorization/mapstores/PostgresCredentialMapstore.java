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

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.MapStoreConfig.InitialLoadMode;
import com.openlattice.authorization.AclKey;
import com.openlattice.directory.MaterializedViewAccount;
import com.openlattice.hazelcast.HazelcastMap;
import com.openlattice.mapstores.TestDataFactory;
import com.geekbeast.postgres.PostgresArrays;
import com.openlattice.postgres.PostgresTable;
import com.openlattice.postgres.ResultSetAdapters;
import com.geekbeast.postgres.mapstores.AbstractBasePostgresMapstore;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import org.apache.commons.lang3.RandomStringUtils;

/**
 * This mapstore assumes that initial first time creation of user in postgres is handled externally.
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class PostgresCredentialMapstore extends AbstractBasePostgresMapstore<AclKey, MaterializedViewAccount> {

    public static final String USERNAME_INDEX = "username";

    public PostgresCredentialMapstore( HikariDataSource hds ) {
        super( HazelcastMap.DB_CREDS, PostgresTable.DB_CREDS, hds );
    }

    @Override public AclKey generateTestKey() {
        return TestDataFactory.aclKey();
    }

    @Override public MaterializedViewAccount generateTestValue() {
        return new MaterializedViewAccount( RandomStringUtils.random( 5 ), RandomStringUtils.random( 5 ) );
    }

    @Override protected void bind( PreparedStatement ps, AclKey key, MaterializedViewAccount value )
            throws SQLException {
        bind( ps, key, 1 );
        ps.setString( 2, value.getUsername() );
        ps.setString( 3, value.getCredential() );

        ps.setString( 4, value.getUsername() );
        ps.setString( 5, value.getCredential() );
    }

    @Override protected int bind( PreparedStatement ps, AclKey key, int parameterIndex ) throws SQLException {
        ps.setArray( parameterIndex++, PostgresArrays.createUuidArray( ps.getConnection(), key ) );
        return parameterIndex;
    }

    @Override public void delete( AclKey key ) {
        super.delete( key );
    }

    @Override public void deleteAll( Collection<AclKey> keys ) {
        super.deleteAll( keys );
    }

    @Override protected MaterializedViewAccount mapToValue( ResultSet rs ) throws SQLException {
        return ResultSetAdapters.materializedViewAccount( rs );
    }

    @Override protected AclKey mapToKey( ResultSet rs ) throws SQLException {
        return ResultSetAdapters.aclKey( rs );
    }

    @Override public MapStoreConfig getMapStoreConfig() {
        return super.getMapStoreConfig()
                .setInitialLoadMode( InitialLoadMode.EAGER );
    }

    @Override public MapConfig getMapConfig() {
        return super
                .getMapConfig()
                .setInMemoryFormat( InMemoryFormat.OBJECT )
                .addIndexConfig( new IndexConfig( IndexType.HASH, USERNAME_INDEX ) );
    }
}
