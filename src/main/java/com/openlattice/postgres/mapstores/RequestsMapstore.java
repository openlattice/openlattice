package com.openlattice.postgres.mapstores;

import static com.openlattice.postgres.PostgresArrays.createTextArray;
import static com.openlattice.postgres.PostgresArrays.createUuidArray;
import static com.openlattice.postgres.PostgresTable.REQUESTS;

import com.openlattice.authorization.AceKey;
import com.openlattice.authorization.Permission;
import com.openlattice.authorization.Principal;
import com.openlattice.hazelcast.HazelcastMap;
import com.openlattice.mapstores.TestDataFactory;
import com.openlattice.requests.Request;
import com.openlattice.requests.Status;
import com.openlattice.authorization.AclKey;
import com.openlattice.postgres.ResultSetAdapters;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;

public class RequestsMapstore extends AbstractBasePostgresMapstore<AceKey, Status> {
    private static final AclKey    TEST_ACL_KEY        = TestDataFactory.aclKey();
    private static final Principal TEST_USER_PRINCIPAL = TestDataFactory.userPrincipal();

    public RequestsMapstore( HikariDataSource hds ) {
        super( HazelcastMap.REQUESTS.name(), REQUESTS, hds );
    }

    @Override protected void bind( PreparedStatement ps, AceKey key, Status value ) throws SQLException {
        bind( ps, key, 1 );

        Array permissions = createTextArray( ps.getConnection(),
                value.getRequest().getPermissions().stream().map( Permission::name ) );
        ps.setArray( 4, permissions );
        ps.setString( 5, value.getRequest().getReason() );
        ps.setString( 6, value.getStatus().name() );

        // UPDATE
        ps.setArray( 7, permissions );
        ps.setString( 8, value.getRequest().getReason() );
        ps.setString( 9, value.getStatus().name() );
    }

    @Override protected int bind( PreparedStatement ps, AceKey key, int parameterIndex ) throws SQLException {
        Principal p = key.getPrincipal();
        ps.setArray( parameterIndex++, createUuidArray( ps.getConnection(), key.getAclKey().stream() ) );
        ps.setString( parameterIndex++, p.getType().name() );
        ps.setString( parameterIndex++, p.getId() );
        return parameterIndex;
    }

    @Override protected Status mapToValue( ResultSet rs ) throws SQLException {
        return ResultSetAdapters.status( rs );
    }

    @Override protected AceKey mapToKey( ResultSet rs ) throws SQLException {
        return ResultSetAdapters.aceKey( rs );
    }

    @Override public AceKey generateTestKey() {
        return new AceKey( TEST_ACL_KEY, TEST_USER_PRINCIPAL );
    }

    @Override public Status generateTestValue() {
        return new Status( new Request(
                TEST_ACL_KEY,
                TestDataFactory.permissions(),
                Optional.of( "Requesting for this object because RandomStringUtils.randomAlphanumeric( 5 )" ) ),
                TEST_USER_PRINCIPAL, TestDataFactory.requestStatus() );
    }
}
