package com.openlattice.postgres.mapstores;

import com.google.common.collect.ImmutableSet;
import com.openlattice.auditing.AuditRecordEntitySetConfiguration;
import com.openlattice.hazelcast.HazelcastMap;
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
        extends AbstractBasePostgresMapstore<UUID, AuditRecordEntitySetConfiguration> {
    public AuditRecordEntitySetConfigurationMapstore(
            HikariDataSource hds ) {
        super( HazelcastMap.AUDIT_RECORD_ENTITY_SETS.name(), PostgresTable.AUDIT_RECORD_ENTITY_SET_IDS, hds );
    }

    @Override protected void bind(
            PreparedStatement ps, UUID key, AuditRecordEntitySetConfiguration value ) throws SQLException {
        int offset = bind( ps, key, 1 );
        final Array arr = PostgresArrays.createUuidArray( ps.getConnection(), value.getAuditRecordEntitySetIds() );
        ps.setObject( offset++, value.getActiveAuditRecordEntitySetId() );
        ps.setObject( offset++, arr );
    }

    @Override protected int bind( PreparedStatement ps, UUID key, int offset ) throws SQLException {
        ps.setObject( offset++, key );
        return offset;
    }

    @Override protected UUID mapToKey( ResultSet rs ) throws SQLException {
        return ResultSetAdapters.id( rs );
    }

    @Override protected AuditRecordEntitySetConfiguration mapToValue( ResultSet rs ) throws SQLException {
        return ResultSetAdapters.auditRecordEntitySetConfiguration( rs );
    }

    @Override public UUID generateTestKey() {
        return UUID.randomUUID();
    }

    @Override public AuditRecordEntitySetConfiguration generateTestValue() {
        return new AuditRecordEntitySetConfiguration( UUID.randomUUID(),
                ImmutableSet.of( UUID.randomUUID(), UUID.randomUUID() ) );
    }
}
