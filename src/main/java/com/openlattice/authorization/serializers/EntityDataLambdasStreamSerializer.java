package com.openlattice.authorization.serializers;

import com.dataloom.mappers.ObjectMappers;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.openlattice.conductor.rpc.EntityDataLambdas;
import java.io.IOException;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EntityDataLambdasStreamSerializer extends Serializer<EntityDataLambdas> {
    private static final Logger        logger = LoggerFactory.getLogger( EntityDataLambdasStreamSerializer.class );
    private              TypeReference ref    = new TypeReference<SetMultimap<UUID, Object>>() {};

    private ObjectMapper mapper;

    public EntityDataLambdasStreamSerializer() {
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
    public void write(
            Kryo kryo, Output output, EntityDataLambdas object ) {
        writeUUID( output, object.getEntitySetId() );
        writeUUID( output, object.getSyncId() );
        output.writeString( object.getEntityId() );
        output.writeBoolean( object.getShouldUpdate() );

        try {
            byte[] bytes = mapper.writeValueAsBytes( object.getPropertyValues() );
            output.writeInt( bytes.length );
            output.writeBytes( bytes );
        } catch ( JsonProcessingException e ) {
            logger.debug( "Unable to serialize entity with id: {}", object.getEntityId() );
        }
    }

    @Override
    public EntityDataLambdas read(
            Kryo kryo, Input input, Class<EntityDataLambdas> type ) {
        UUID entitySetId = readUUID( input );
        UUID syncId = readUUID( input );
        String entityId = input.readString();
        boolean shouldUpdate = input.readBoolean();

        int numBytes = input.readInt();
        SetMultimap<UUID, Object> propertyValues = HashMultimap.create();
        try {
            propertyValues = mapper.readValue(input.readBytes( numBytes ), ref );
        } catch ( IOException e ) {
            logger.debug( "Unable to deserialize entity with id: {}", entityId );
        }

        return new EntityDataLambdas( entitySetId, syncId, entityId, propertyValues, shouldUpdate );
    }
}
