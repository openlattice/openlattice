package com.openlattice.hazelcast.serializers;

import com.dataloom.mappers.ObjectMappers;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.openlattice.authorization.serializers.EntityDataLambdasStreamSerializer;
import com.openlattice.conductor.rpc.BulkEntityDataLambdas;

import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Component
public class BulkEntityDataLambdasStreamSerializer extends Serializer<BulkEntityDataLambdas> {
    private static final Logger logger = LoggerFactory.getLogger( EntityDataLambdasStreamSerializer.class );
    private TypeReference ref = new TypeReference<SetMultimap<UUID, Object>>() {
    };

    private ObjectMapper mapper;

    public BulkEntityDataLambdasStreamSerializer() {
        this.mapper = ObjectMappers.getSmileMapper();
    }

    private void writeUUID( Output output, UUID id ) {
        output.writeLong( id.getLeastSignificantBits() );
        output.writeLong( id.getMostSignificantBits() );
    }

    private UUID readUUID( Input input ) {
        long lsb = input.readLong();
        long msb = input.readLong();
        return new UUID( msb, lsb );
    }

    @Override
    public void write( Kryo kryo, Output output, BulkEntityDataLambdas object ) {
        writeUUID( output, object.getEntitySetId() );

        try {
            output.writeInt( object.getEntitiesById().size() );

            for ( Map.Entry<UUID, Map<UUID, Set<Object>>> entry : object.getEntitiesById().entrySet() ) {
                writeUUID( output, entry.getKey() );
                byte[] bytes = mapper.writeValueAsBytes( entry.getValue() );
                output.writeInt( bytes.length );
                output.writeBytes( bytes );
            }
        } catch ( JsonProcessingException e ) {
            logger.debug( "Unable to serialize entity with for entity set: {}", object.getEntitySetId() );
        }
    }

    @Override
    public BulkEntityDataLambdas read(
            Kryo kryo, Input input, Class<BulkEntityDataLambdas> type ) {
        UUID entitySetId = readUUID( input );

        int entitiesSize = input.readInt();
        Map<UUID, SetMultimap<UUID, Object>> entitiesById = new HashMap<>( entitiesSize );
        for ( int j = 0; j < entitiesSize; j++ ) {
            UUID entityId = readUUID( input );

            int numBytes = input.readInt();
            HashMultimap<UUID, Object> entityData;
            try {
                entityData = mapper.readValue( input.readBytes( numBytes ), ref );
                entitiesById.put( entityId, entityData );
            } catch ( IOException e ) {
                logger.debug( "Unable to deserialize entities for entity set: {}", entitySetId );
            }
        }

        return new BulkEntityDataLambdas( entitySetId, Maps.transformValues( entitiesById, Multimaps::asMap ) );
    }
}
