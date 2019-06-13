package com.openlattice.postgres.mapstores;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapIndexConfig;
import com.openlattice.auditing.AuditRecordEntitySetConfiguration;
import com.openlattice.authorization.AclKey;
import com.openlattice.hazelcast.HazelcastMap;
import com.openlattice.mapstores.TestDataFactory;
import com.openlattice.postgres.PostgresArrays;
import com.openlattice.postgres.PostgresTable;
import com.openlattice.postgres.ResultSetAdapters;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;

/**
 *
 */
public class AuditRecordEntitySetConfigurationMapstore
        extends AbstractBasePostgresMapstore<AclKey, AuditRecordEntitySetConfiguration> {
    public static final String ANY_AUDITING_ENTITY_SETS      = "auditRecordEntitySetIds[any]";
    public static final String ANY_EDGE_AUDITING_ENTITY_SETS = "auditEdgeEntitySetIds[any]";

    public AuditRecordEntitySetConfigurationMapstore(
            HikariDataSource hds ) {
        super( HazelcastMap.AUDIT_RECORD_ENTITY_SETS.name(), PostgresTable.AUDIT_RECORD_ENTITY_SET_IDS, hds );
    }

    @Override protected void bind(
            PreparedStatement ps, AclKey key, AuditRecordEntitySetConfiguration value ) throws SQLException {
        int offset = bind( ps, key, 1 );
        final Array arr = PostgresArrays.createUuidArray( ps.getConnection(), value.getAuditRecordEntitySetIds() );
        final Array arrEdge = PostgresArrays.createUuidArray( ps.getConnection(), value.getAuditEdgeEntitySetIds() );
        ps.setObject( offset++, value.getActiveAuditRecordEntitySetId() );
        ps.setObject( offset++, value.getActiveAuditEdgeEntitySetId() );
        ps.setObject( offset++, arr );
        ps.setObject( offset++, arrEdge );

        //Handle update clause.
        ps.setObject( offset++, value.getActiveAuditRecordEntitySetId() );
        ps.setObject( offset++, value.getActiveAuditEdgeEntitySetId() );
        ps.setObject( offset++, arr );
        ps.setObject( offset++, arrEdge );
    }

    @Override protected int bind( PreparedStatement ps, AclKey key, int offset ) throws SQLException {
        final Array arr = PostgresArrays.createUuidArray( ps.getConnection(), key );
        ps.setObject( offset++, arr );
        return offset;
    }

    @Override protected AclKey mapToKey( ResultSet rs ) throws SQLException {
        return ResultSetAdapters.aclKey( rs );
    }

    @Override protected AuditRecordEntitySetConfiguration mapToValue( ResultSet rs ) throws SQLException {
        return ResultSetAdapters.auditRecordEntitySetConfiguration( rs );
    }

    @Override public MapConfig getMapConfig() {
        return super.getMapConfig()
                .addMapIndexConfig( new MapIndexConfig( ANY_AUDITING_ENTITY_SETS, false ) )
                .addMapIndexConfig( new MapIndexConfig( ANY_EDGE_AUDITING_ENTITY_SETS, false ) );
    }

    @Override public AclKey generateTestKey() {
        return TestDataFactory.aclKey();
    }

    @Override public AuditRecordEntitySetConfiguration generateTestValue() {
        return new AuditRecordEntitySetConfiguration( UUID.randomUUID(), UUID.randomUUID(),
                ImmutableList.of( UUID.randomUUID(), UUID.randomUUID() ),
                ImmutableList.of( UUID.randomUUID(), UUID.randomUUID() ) );
    }
}
