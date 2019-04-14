package com.openlattice.hazelcast.serializers;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.openlattice.conductor.rpc.SearchWithConstraintsLambda;
import com.openlattice.rhizome.hazelcast.DelegatedUUIDSet;
import com.openlattice.search.requests.SearchConstraints;

import java.util.*;

public class SearchWithConstraintsLambdaStreamSerializer extends Serializer<SearchWithConstraintsLambda> {

    private static void writeUUID( Output output, UUID id ) {
        output.writeLong( id.getLeastSignificantBits() );
        output.writeLong( id.getMostSignificantBits() );
    }

    private static UUID readUUID( Input input ) {
        long lsb = input.readLong();
        long msb = input.readLong();
        return new UUID( msb, lsb );
    }

    private static void writeUUIDToSetUUIDMap( Output output, Map<UUID, DelegatedUUIDSet> map ) {
        output.writeInt( map.size() );
        for ( Map.Entry<UUID, DelegatedUUIDSet> entry : map.entrySet() ) {
            writeUUID( output, entry.getKey() );

            output.writeInt( entry.getValue().size() );

            for ( UUID propertyTypeId : entry.getValue() ) {
                writeUUID( output, propertyTypeId );
            }
        }
    }

    private static Map<UUID, DelegatedUUIDSet> readUUIDToSetUUIDMap( Input input ) {
        int mapSize = input.readInt();
        Map<UUID, DelegatedUUIDSet> map = new HashMap<>( mapSize );
        for ( int i = 0; i < mapSize; i++ ) {
            UUID key = readUUID( input );

            int numValues = input.readInt();
            Set<UUID> values = new HashSet<>( numValues );

            for ( int j = 0; j < numValues; j++ ) {
                values.add( readUUID( input ) );
            }

            map.put( key, DelegatedUUIDSet.wrap( values ) );
        }

        return map;
    }

    @Override
    public void write(
            Kryo kryo, Output output, SearchWithConstraintsLambda object ) {

        SearchConstraintsStreamSerializer.serialize( output, object.getSearchConstraints() );

        output.writeInt( (object.getEntityTypesByEntitySetId().size() ) );
        for ( Map.Entry<UUID, UUID> entry : object.getEntityTypesByEntitySetId().entrySet()) {
            writeUUID( output, entry.getKey() );
            writeUUID( output, entry.getValue() );
        }

        writeUUIDToSetUUIDMap( output, object.getAuthorizedProperties() );

        writeUUIDToSetUUIDMap( output, object.getLinkingEntitySets() );
    }

    @Override
    public SearchWithConstraintsLambda read(
            Kryo kryo, Input input, Class<SearchWithConstraintsLambda> type ) {

        SearchConstraints searchConstraints = SearchConstraintsStreamSerializer.deserialize( input );

        int entitySetsToEntityTypesMapSize = input.readInt();
        Map<UUID, UUID> entitySetsToEntityTypes = new HashMap<>( entitySetsToEntityTypesMapSize );
        for ( int i = 0; i < entitySetsToEntityTypesMapSize; i++ ) {
            UUID entitySetId = readUUID( input );
            UUID entityTypeId = readUUID( input );
            entitySetsToEntityTypes.put( entitySetId, entityTypeId );
        }

        Map<UUID, DelegatedUUIDSet> authorizedProperties = readUUIDToSetUUIDMap(input);

        Map<UUID, DelegatedUUIDSet> linkingEntitySets = readUUIDToSetUUIDMap( input );

        return new SearchWithConstraintsLambda( searchConstraints, entitySetsToEntityTypes, authorizedProperties, linkingEntitySets );
    }
}
