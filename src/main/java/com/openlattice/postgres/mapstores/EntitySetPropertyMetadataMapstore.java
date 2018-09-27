package com.openlattice.postgres.mapstores;

import static com.openlattice.postgres.PostgresTable.ENTITY_SET_PROPERTY_METADATA;

import com.openlattice.edm.set.EntitySetPropertyKey;
import com.openlattice.edm.set.EntitySetPropertyMetadata;
import com.openlattice.hazelcast.HazelcastMap;
import com.openlattice.postgres.PostgresArrays;
import com.openlattice.postgres.ResultSetAdapters;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.UUID;
import org.apache.commons.lang3.RandomStringUtils;

public class EntitySetPropertyMetadataMapstore
        extends AbstractBasePostgresMapstore<EntitySetPropertyKey, EntitySetPropertyMetadata> {

    public EntitySetPropertyMetadataMapstore( HikariDataSource hds ) {
        super( HazelcastMap.ENTITY_SET_PROPERTY_METADATA.name(), ENTITY_SET_PROPERTY_METADATA, hds );
    }

    @Override protected void bind(
            PreparedStatement ps, EntitySetPropertyKey key, EntitySetPropertyMetadata value ) throws SQLException {
        var parameterIndex = bind( ps, key, 1 );

        final var tags = PostgresArrays.createTextArray( ps.getConnection(), value.getTags() );

        ps.setString( parameterIndex++, value.getTitle() );
        ps.setString( parameterIndex++, value.getDescription() );
        ps.setArray( parameterIndex++, tags );
        ps.setBoolean( parameterIndex++, value.getDefaultShow() );

        ps.setString( parameterIndex++, value.getTitle() );
        ps.setString( parameterIndex++, value.getDescription() );
        ps.setArray( parameterIndex++, tags );
        ps.setBoolean( parameterIndex++, value.getDefaultShow() );
    }

    @Override protected int bind( PreparedStatement ps, EntitySetPropertyKey key, int parameterIndex )
            throws SQLException {
        ps.setObject( parameterIndex++, key.getEntitySetId() );
        ps.setObject( parameterIndex++, key.getPropertyTypeId() );
        return parameterIndex;
    }

    @Override protected EntitySetPropertyMetadata mapToValue( ResultSet rs ) throws SQLException {
        return ResultSetAdapters.entitySetPropertyMetadata( rs );
    }

    @Override protected EntitySetPropertyKey mapToKey( ResultSet rs ) throws SQLException {
        return ResultSetAdapters.entitySetPropertyKey( rs );
    }

    @Override public EntitySetPropertyKey generateTestKey() {
        return new EntitySetPropertyKey( UUID.randomUUID(), UUID.randomUUID() );
    }

    @Override public EntitySetPropertyMetadata generateTestValue() {
        return new EntitySetPropertyMetadata( RandomStringUtils.random( 10 ),
                RandomStringUtils.random( 10 ),
                new LinkedHashSet<>( Arrays
                        .asList( RandomStringUtils.random( 5 ), RandomStringUtils.random( 5 ) ) ),
                true );
    }
}
