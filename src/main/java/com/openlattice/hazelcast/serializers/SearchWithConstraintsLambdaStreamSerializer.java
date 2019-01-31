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

    @Override
    public void write(
            Kryo kryo, Output output, SearchWithConstraintsLambda object ) {

        SearchConstraintsStreamSerializer.serialize( output, object.getSearchConstraints() );

        output.writeInt( object.getAuthorizedProperties().size() );
        for ( Map.Entry<UUID, DelegatedUUIDSet> entry : object.getAuthorizedProperties().entrySet() ) {
            writeUUID( output, entry.getKey() );

            output.writeInt( entry.getValue().size() );

            for ( UUID propertyTypeId : entry.getValue() ) {
                writeUUID( output, propertyTypeId );
            }
        }
        output.writeBoolean( object.isLinking() );
    }

    @Override
    public SearchWithConstraintsLambda read(
            Kryo kryo, Input input, Class<SearchWithConstraintsLambda> type ) {

        SearchConstraints searchConstraints = SearchConstraintsStreamSerializer.deserialize( input );

        int numEntitySets = input.readInt();

        Map<UUID, DelegatedUUIDSet> authorizedProperties = new HashMap<>( numEntitySets );

        for ( int i = 0; i < numEntitySets; i++ ) {
            UUID entitySetId = readUUID( input );

            int numPropertyTypes = input.readInt();
            Set<UUID> propertyTypeIds = new HashSet<>( numPropertyTypes );

            for ( int j = 0; j < numPropertyTypes; j++ ) {
                propertyTypeIds.add( readUUID( input ) );
            }

            authorizedProperties.put( entitySetId, DelegatedUUIDSet.wrap( propertyTypeIds ) );
        }
        boolean linking = input.readBoolean();

        return new SearchWithConstraintsLambda( searchConstraints, authorizedProperties, linking );
    }
}
