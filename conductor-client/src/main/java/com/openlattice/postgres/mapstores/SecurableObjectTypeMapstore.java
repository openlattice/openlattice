package com.openlattice.postgres.mapstores;

import static com.openlattice.postgres.PostgresTable.SECURABLE_OBJECTS;

import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.config.MapConfig;
import com.openlattice.authorization.AclKey;
import com.openlattice.authorization.securable.SecurableObjectType;
import com.openlattice.hazelcast.HazelcastMap;
import com.openlattice.postgres.PostgresArrays;
import com.openlattice.postgres.ResultSetAdapters;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;

public class SecurableObjectTypeMapstore extends AbstractBasePostgresMapstore<AclKey, SecurableObjectType> {
    public static final String ACL_KEY_INDEX               = "__key.index";
    public static final String SECURABLE_OBJECT_TYPE_INDEX = "this";

    public SecurableObjectTypeMapstore( HikariDataSource hds ) {
        super( HazelcastMap.SECURABLE_OBJECT_TYPES, SECURABLE_OBJECTS, hds );
    }

    @Override protected void bind(
            PreparedStatement ps, AclKey key, SecurableObjectType value ) throws SQLException {
        bind( ps, key, 1 );
        ps.setString( 2, value.name() );
        ps.setString( 3, value.name() );
    }

    @Override protected int bind( PreparedStatement ps, AclKey key, int parameterIndex ) throws SQLException {
        ps.setArray( parameterIndex++, PostgresArrays.createUuidArray( ps.getConnection(), key ) );
        return parameterIndex;
    }

    @Override protected SecurableObjectType mapToValue( ResultSet rs ) throws SQLException {
        SecurableObjectType objectType = ResultSetAdapters.securableObjectType( rs );
        return objectType == null ? SecurableObjectType.Unknown : objectType;
    }

    @Override protected AclKey mapToKey( ResultSet rs ) throws SQLException {
        return ResultSetAdapters.aclKey( rs );
    }

    @Override public MapConfig getMapConfig() {
        return super.getMapConfig()
                .addIndexConfig( new IndexConfig( IndexType.HASH, ACL_KEY_INDEX ) )
                .addIndexConfig( new IndexConfig( IndexType.HASH, SECURABLE_OBJECT_TYPE_INDEX ) );
    }

    @Override public AclKey generateTestKey() {
        return new AclKey( UUID.randomUUID(), UUID.randomUUID() );
    }

    @Override public SecurableObjectType generateTestValue() {
        return SecurableObjectType.EntitySet;
    }
}
