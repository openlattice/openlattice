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

import static com.openlattice.postgres.PostgresArrays.createTextArray;
import static com.openlattice.postgres.PostgresArrays.createUuidArray;

import com.codahale.metrics.annotation.Timed;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapIndexConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.MapStoreConfig.InitialLoadMode;
import com.openlattice.authorization.AceKey;
import com.openlattice.authorization.AceValue;
import com.openlattice.authorization.AclKey;
import com.openlattice.authorization.Permission;
import com.openlattice.authorization.Principal;
import com.openlattice.authorization.PrincipalType;
import com.openlattice.authorization.securable.SecurableObjectType;
import com.openlattice.hazelcast.HazelcastMap;
import com.openlattice.postgres.PostgresTable;
import com.openlattice.postgres.ResultSetAdapters;
import com.openlattice.postgres.mapstores.AbstractBasePostgresMapstore;
import com.openlattice.postgres.mapstores.SecurableObjectTypeMapstore;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.EnumSet;
import java.util.UUID;

import org.apache.commons.lang3.RandomStringUtils;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class PermissionMapstore extends AbstractBasePostgresMapstore<AceKey, AceValue> {
    public static final String PRINCIPAL_INDEX             = "__key#principal";
    public static final String PRINCIPAL_TYPE_INDEX        = "__key#principal.type";
    public static final String SECURABLE_OBJECT_TYPE_INDEX = "securableObjectType";
    public static final String PERMISSIONS_INDEX           = "permissions[any]";
    public static final String ACL_KEY_INDEX               = "__key#aclKey.index";
    public static final String EXPIRATION_DATE_INDEX       = "expirationDate";

    private final SecurableObjectTypeMapstore objectTypes;

    public PermissionMapstore( HikariDataSource hds ) {
        super( HazelcastMap.PERMISSIONS.name(), PostgresTable.PERMISSIONS, hds );
        this.objectTypes = new SecurableObjectTypeMapstore( hds );
    }

    @Override protected void bind(
            PreparedStatement ps, AceKey key, AceValue value ) throws SQLException {
        bind( ps, key, 1 );
        Array permissions = createTextArray(
                ps.getConnection(),
                value.getPermissions().stream().map( Permission::name ) );
        OffsetDateTime expirationDate = value.getExpirationDate();
        ps.setArray( 4, permissions );
        ps.setObject( 5, expirationDate );
        ps.setArray( 6, permissions );
        ps.setObject( 7, expirationDate );
    }

    @Override
    protected int bind( PreparedStatement ps, AceKey key, int parameterIndex ) throws SQLException {
        Principal p = key.getPrincipal();
        ps.setArray( parameterIndex++, createUuidArray( ps.getConnection(), key.getAclKey().stream() ) );
        ps.setString( parameterIndex++, p.getType().name() );
        ps.setString( parameterIndex++, p.getId() );
        return parameterIndex;
    }

    @Timed
    @Override
    protected AceValue mapToValue( ResultSet rs ) throws SQLException {
        EnumSet<Permission> permissions = ResultSetAdapters.permissions( rs );
        AclKey aclKey = ResultSetAdapters.aclKey( rs );
        OffsetDateTime expirationDate = ResultSetAdapters.expirationDate( rs );
        /*
         * There is small risk of deadlock here if all readers get stuck waiting for connection from the connection pool
         * we should keep an eye out to make sure there aren't an unusual number of TimeoutExceptions being thrown.
         */
        SecurableObjectType objectType = objectTypes.load( aclKey );

        if ( objectType == null ) {
            logger.warn( "SecurableObjectType was null for key {}", aclKey );
        }
        return new AceValue( permissions, objectType, expirationDate );
    }

    @Override protected AceKey mapToKey( ResultSet rs ) throws SQLException {
        return ResultSetAdapters.aceKey( rs );
    }

    @Override public MapStoreConfig getMapStoreConfig() {
        return super
                .getMapStoreConfig()
                .setInitialLoadMode( InitialLoadMode.EAGER );
    }

    @Override public MapConfig getMapConfig() {
        return super
                .getMapConfig()
                .setInMemoryFormat( InMemoryFormat.OBJECT )
                .addMapIndexConfig( new MapIndexConfig( ACL_KEY_INDEX, false ) )
                .addMapIndexConfig( new MapIndexConfig( PRINCIPAL_INDEX, false ) )
                .addMapIndexConfig( new MapIndexConfig( PRINCIPAL_TYPE_INDEX, false ) )
                .addMapIndexConfig( new MapIndexConfig( SECURABLE_OBJECT_TYPE_INDEX, false ) )
                .addMapIndexConfig( new MapIndexConfig( PERMISSIONS_INDEX, false ) )
                .addMapIndexConfig( new MapIndexConfig( EXPIRATION_DATE_INDEX, true ) );
    }

    @Override
    public AceKey generateTestKey() {
        return new AceKey(
                new AclKey( UUID.randomUUID() ),
                new Principal( PrincipalType.USER, RandomStringUtils.randomAlphanumeric( 5 ) ) );
    }

    @Override
    public AceValue generateTestValue() {
        return new AceValue(
                EnumSet.of( Permission.READ, Permission.WRITE ),
                SecurableObjectType.PropertyTypeInEntitySet );
    }
}
