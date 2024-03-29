package com.openlattice.postgres.mapstores;

import com.geekbeast.postgres.mapstores.AbstractBasePostgresMapstore;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.config.MapConfig;
import com.openlattice.edm.type.AssociationType;
import com.openlattice.hazelcast.HazelcastMap;
import com.google.common.collect.Sets;
import com.geekbeast.postgres.PostgresArrays;
import com.openlattice.postgres.ResultSetAdapters;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Optional;
import java.util.UUID;

import static com.openlattice.postgres.PostgresTable.ASSOCIATION_TYPES;

public class AssociationTypeMapstore extends AbstractBasePostgresMapstore<UUID, AssociationType> {

    public static final String ID_INDEX = "__key";
    public static final String SRC_INDEX = "src[any]";
    public static final String DST_INDEX = "dst[any]";

    public AssociationTypeMapstore( HikariDataSource hds ) {
        super( HazelcastMap.ASSOCIATION_TYPES, ASSOCIATION_TYPES, hds );
    }

    @Override protected void bind( PreparedStatement ps, UUID key, AssociationType value ) throws SQLException {
        bind( ps, key, 1);
        Array src = PostgresArrays.createUuidArray( ps.getConnection(), value.getSrc().stream() );
        Array dst = PostgresArrays.createUuidArray( ps.getConnection(), value.getDst().stream() );

        ps.setArray( 2, src );
        ps.setArray( 3, dst );
        ps.setBoolean( 4, value.isBidirectional() );

        // UPDATE

        ps.setArray( 5, src );
        ps.setArray( 6, dst );
        ps.setBoolean( 7, value.isBidirectional() );

    }

    @Override protected int bind( PreparedStatement ps, UUID key, int parameterIndex ) throws SQLException {
        ps.setObject( parameterIndex++, key );
        return parameterIndex;
    }

    @Override protected AssociationType mapToValue( ResultSet rs ) throws SQLException {
        return ResultSetAdapters.associationType( rs );
    }

    @Override protected UUID mapToKey( ResultSet rs ) throws SQLException {
        return ResultSetAdapters.id( rs );
    }

    @Override public MapConfig getMapConfig() {
        return super.getMapConfig()
                .addIndexConfig( new IndexConfig( IndexType.HASH, ID_INDEX) )
                .addIndexConfig( new IndexConfig( IndexType.HASH, SRC_INDEX) )
                .addIndexConfig( new IndexConfig( IndexType.HASH, DST_INDEX) );
    }

    @Override public UUID generateTestKey() {
        return UUID.randomUUID();
    }

    @Override public AssociationType generateTestValue() {
        return new AssociationType(
                Optional.empty(),
                Sets.newLinkedHashSet( Arrays.asList( UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID() ) ),
                Sets.newLinkedHashSet( Arrays.asList( UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID() ) ),
                false );
    }
}
