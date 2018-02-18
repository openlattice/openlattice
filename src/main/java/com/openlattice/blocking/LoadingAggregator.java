package com.openlattice.blocking;

import com.dataloom.mappers.ObjectMappers;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IMap;
import com.openlattice.data.hazelcast.DataKey;
import com.openlattice.datastore.cassandra.CassandraSerDesFactory;
import com.openlattice.edm.type.PropertyType;
import com.openlattice.hazelcast.HazelcastMap;
import com.openlattice.rhizome.hazelcast.DelegatedStringSet;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;

public class LoadingAggregator
        extends Aggregator<Map.Entry<DataKey, ByteBuffer>, Integer>
        implements HazelcastInstanceAware {
    private static final long serialVersionUID = -8998522786390338940L;

    private final Map<GraphEntityPair, LinkingEntity> entities;
    private final ObjectMapper mapper = ObjectMappers.getJsonMapper();

    @SuppressFBWarnings( value = "SE_BAD_FIELD", justification = "Custom Stream Serializer is implemented" )
    private final     Map<UUID, Map<UUID, PropertyType>>   authorizedPropertyTypes;
    private final     UUID                                 graphId;
    private transient IMap<GraphEntityPair, LinkingEntity> linkingEntities;

    public LoadingAggregator(
            UUID graphId,
            Map<UUID, Map<UUID, PropertyType>> authorizedPropertyTypes,
            Map<GraphEntityPair, LinkingEntity> entities ) {
        this.graphId = graphId;
        this.authorizedPropertyTypes = authorizedPropertyTypes;
        this.entities = entities;
    }

    public LoadingAggregator( UUID graphId, Map<UUID, Map<UUID, PropertyType>> authorizedPropertyTypes ) {
        this( graphId, authorizedPropertyTypes, Maps.newHashMap() );
    }

    @Override public void accumulate( Map.Entry<DataKey, ByteBuffer> input ) {
        DataKey key = input.getKey();
        UUID entityKeyId = key.getId();
        UUID propertyTypeId = key.getPropertyTypeId();
        UUID entitySetId = key.getEntitySetId();
        if ( authorizedPropertyTypes.get( entitySetId ).containsKey( propertyTypeId ) ) {
            GraphEntityPair graphEntityPair = new GraphEntityPair( graphId, entityKeyId );
            String value = CassandraSerDesFactory.deserializeValue( mapper,
                    input.getValue(),
                    authorizedPropertyTypes.get( entitySetId ).get( propertyTypeId ).getDatatype(),
                    key.getEntityId() ).toString();
            LinkingEntity entity = ( entities.containsKey( graphEntityPair ) ) ?
                    entities.get( graphEntityPair ) :
                    new LinkingEntity( Maps.newHashMap() );
            entity.addEntry( propertyTypeId, value );
            entities.put( graphEntityPair, entity );
        }
    }

    @Override public void combine( Aggregator aggregator ) {
        if ( aggregator instanceof LoadingAggregator ) {
            LoadingAggregator other = (LoadingAggregator) aggregator;
            other.entities.entrySet().stream().forEach( entry -> {
                GraphEntityPair graphEntityPair = entry.getKey();
                LinkingEntity entity = entry.getValue();
                if ( !entities.containsKey( graphEntityPair ) ) {
                    entities.put( graphEntityPair, entity );
                } else {
                    LinkingEntity existingEntity = entities.get( graphEntityPair );
                    entity.getEntity().entrySet().stream().forEach( valueEntry -> {
                        UUID propertyTypeId = valueEntry.getKey();
                        DelegatedStringSet values = valueEntry.getValue();
                        existingEntity.addEntry( propertyTypeId, values );
                    } );
                    entities.put( graphEntityPair, existingEntity );
                }
            } );
        }
    }

    @Override public Integer aggregate() {
        linkingEntities.putAll( entities );
        return entities.size();
    }

    @Override public void setHazelcastInstance( HazelcastInstance hazelcastInstance ) {
        this.linkingEntities = hazelcastInstance.getMap( HazelcastMap.LINKING_ENTITIES.name() );
    }

    public Map<UUID, Map<UUID, PropertyType>> getAuthorizedPropertyTypes() {
        return authorizedPropertyTypes;
    }

    public Map<GraphEntityPair, LinkingEntity> getEntities() {
        return entities;
    }

    public UUID getGraphId() {
        return graphId;
    }

    @Override public boolean equals( Object o ) {
        if ( this == o ) { return true; }
        if ( o == null || getClass() != o.getClass() ) { return false; }

        LoadingAggregator that = (LoadingAggregator) o;

        return authorizedPropertyTypes.equals( that.authorizedPropertyTypes );
    }

    @Override public int hashCode() {
        return authorizedPropertyTypes.hashCode();
    }

}
