package com.openlattice.postgres.mapstores;

import com.openlattice.hazelcast.HazelcastMap;
import com.openlattice.linking.LinkingVertexKey;
import com.openlattice.linking.WeightedLinkingVertexKey;
import com.openlattice.linking.WeightedLinkingVertexKeySet;
import com.google.common.collect.ImmutableList;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapIndexConfig;
import com.hazelcast.config.MapStoreConfig;
import com.openlattice.postgres.PostgresColumnDefinition;
import com.zaxxer.hikari.HikariDataSource;
import java.util.Optional;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.math.RandomUtils;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.openlattice.postgres.PostgresColumn.*;
import static com.openlattice.postgres.PostgresTable.LINKING_EDGES;

public class LinkingEdgesMapstore extends AbstractBasePostgresMapstore<LinkingVertexKey, WeightedLinkingVertexKeySet> {
    public LinkingEdgesMapstore( HikariDataSource hds ) {
        super( HazelcastMap.LINKING_EDGES.name(), LINKING_EDGES, hds );
    }

    @Override protected Optional<String> buildOnConflictQuery() {
        return Optional.empty();
    }

    @Override protected void bind(
            PreparedStatement ps, LinkingVertexKey key, WeightedLinkingVertexKeySet value ) throws SQLException {
        throw new NotImplementedException( "Something went very wrong. This should never happen." );
    }

    @Override protected int bind( PreparedStatement ps, LinkingVertexKey key, int parameterIndex ) throws SQLException {
        throw new NotImplementedException( "Something went very wrong. This should never happen." );
    }

    @Override protected WeightedLinkingVertexKeySet mapToValue( ResultSet rs ) throws SQLException {
        throw new NotImplementedException( "Something went very wrong. This should never happen." );
    }

    @Override protected LinkingVertexKey mapToKey( ResultSet rs ) {
        throw new NotImplementedException( "Something went very wrong. This should never happen." );
    }

    @Override public MapConfig getMapConfig() {
        return super.getMapConfig()
                .addMapIndexConfig( new MapIndexConfig( "__key#graphId", false ) )
                .addMapIndexConfig( new MapIndexConfig( "__key#vertexId", false ) )
                .addMapIndexConfig( new MapIndexConfig( "value[any].vertexKey", false ) );
    }

    @Override public MapStoreConfig getMapStoreConfig() {
        return super.getMapStoreConfig()
                .setEnabled( false );
    }

    @Override
    public void delete( LinkingVertexKey key ) {
        throw new NotImplementedException( "Something went very wrong. This should never happen." );
    }

    @Override
    public void deleteAll( Collection<LinkingVertexKey> keys ) {
        throw new NotImplementedException( "Something went very wrong. This should never happen." );
    }

    @Override
    public void store( LinkingVertexKey key, WeightedLinkingVertexKeySet value ) {
        throw new NotImplementedException( "Something went very wrong. This should never happen." );
    }

    @Override
    public void storeAll( Map<LinkingVertexKey, WeightedLinkingVertexKeySet> entries ) {
        throw new NotImplementedException( "Something went very wrong. This should never happen." );
    }

    @Override
    public WeightedLinkingVertexKeySet load( LinkingVertexKey key ) {
        throw new NotImplementedException( "Something went very wrong. This should never happen." );
    }

    @Override
    public Map<LinkingVertexKey, WeightedLinkingVertexKeySet> loadAll( Collection<LinkingVertexKey> keys ) {
        throw new NotImplementedException( "Something went very wrong. This should never happen." );
    }

    @Override public LinkingVertexKey generateTestKey() {
        return new LinkingVertexKey( UUID.randomUUID(), UUID.randomUUID() );
    }

    @Override public WeightedLinkingVertexKeySet generateTestValue() {
        WeightedLinkingVertexKeySet val = new WeightedLinkingVertexKeySet();
        val.add( new WeightedLinkingVertexKey( RandomUtils.nextDouble(), new LinkingVertexKey( UUID.randomUUID(), UUID.randomUUID() ) ) );
        return val;
    }
}
