/*
 * Copyright (C) 2018. OpenLattice, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * You can contact the owner of the copyright at support@openlattice.com
 */

package com.openlattice.data.requests;

import com.openlattice.client.serialization.SerializationConstants;
import com.openlattice.data.EntityKey;
import com.openlattice.data.serializers.FullQualifedNameJacksonDeserializer;
import com.openlattice.data.serializers.FullQualifedNameJacksonSerializer;
import com.dataloom.mappers.ObjectMappers;
import com.esotericsoftware.kryo.Kryo;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import de.javakaffee.kryoserializers.UUIDSerializer;
import de.javakaffee.kryoserializers.guava.HashMultimapSerializer;
import de.javakaffee.kryoserializers.guava.ImmutableMultimapSerializer;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

public class BulkDataCreation implements Serializable {

    private static final ObjectMapper      mapper          = ObjectMappers.getSmileMapper();
    private static final ThreadLocal<Kryo> kryoThreadLocal = ThreadLocal.withInitial( () -> {

        Kryo kryo = new Kryo();
        kryo.register( UUID.class, new UUIDSerializer() );
        HashMultimapSerializer.registerSerializers( kryo );
        ImmutableMultimapSerializer.registerSerializers( kryo );
        return kryo;
    } );

    static {
        FullQualifedNameJacksonSerializer.registerWithMapper( ObjectMappers.getJsonMapper() );
        FullQualifedNameJacksonDeserializer.registerWithMapper( ObjectMappers.getJsonMapper() );
    }

    private Set<UUID>        tickets;
    private Set<Entity>      entities;
    private Set<Association> associations;

    @JsonCreator
    public BulkDataCreation(
            @JsonProperty( SerializationConstants.SYNC_TICKETS ) Set<UUID> tickets,
            @JsonProperty( SerializationConstants.ENTITIES ) Set<Entity> entities,
            @JsonProperty( SerializationConstants.ASSOCIATIONS ) Set<Association> associations ) {
        this.tickets = tickets;
        this.entities = entities;
        this.associations = associations;
    }

    @JsonProperty( SerializationConstants.SYNC_TICKETS )
    public Set<UUID> getTickets() {
        return tickets;
    }

    @JsonProperty( SerializationConstants.ENTITIES )
    public Set<Entity> getEntities() {
        return entities;
    }

    @JsonProperty( SerializationConstants.ASSOCIATIONS )
    public Set<Association> getAssociations() {
        return associations;
    }

    private void writeObject( ObjectOutputStream oos )
            throws IOException {
        oos.writeInt( tickets.size() );

        for ( UUID ticket : tickets ) {
            BulkDataCreation.serialize( oos, ticket );
        }

        oos.writeInt( entities.size() );
        for ( Entity entity : entities ) {
            serialize( oos, entity.getKey() );
            serialize( oos, entity.getDetails() );
        }

        oos.writeInt( associations.size() );
        for ( Association association : associations ) {
            serialize( oos, association.getSrc() );
            serialize( oos, association.getDst() );
            serialize( oos, association.getKey() );
            serialize( oos, association.getDetails() );
        }

    }

    private void readObject( ObjectInputStream ois ) throws IOException, ClassNotFoundException {
        int ticketCount = ois.readInt();
        tickets = new HashSet<>();
        entities = new HashSet<>();
        associations = new HashSet<>();

        for ( int i = 0; i < ticketCount; ++i ) {
            tickets.add( deserializeUUID( ois ) );
        }

        int entityCount = ois.readInt();
        for ( int i = 0; i < entityCount; ++i ) {
            EntityKey ek = deserializeEntityKey( ois );
            SetMultimap<UUID, Object> details = deserializeEntityDetails( ois );
            entities.add( new Entity( ek, details ) );
        }

        int associationCount = ois.readInt();
        for ( int i = 0; i < associationCount; ++i ) {
            EntityKey src = deserializeEntityKey( ois );
            EntityKey dst = deserializeEntityKey( ois );
            EntityKey key = deserializeEntityKey( ois );
            SetMultimap<UUID, Object> details = deserializeEntityDetails( ois );
            associations.add( new Association( key, src, dst, details ) );
        }
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ( ( associations == null ) ? 0 : associations.hashCode() );
        result = prime * result + ( ( entities == null ) ? 0 : entities.hashCode() );
        result = prime * result + ( ( tickets == null ) ? 0 : tickets.hashCode() );
        return result;
    }

    @Override
    public boolean equals( Object obj ) {
        if ( this == obj ) { return true; }
        if ( obj == null ) { return false; }
        if ( getClass() != obj.getClass() ) { return false; }
        BulkDataCreation other = (BulkDataCreation) obj;
        if ( associations == null ) {
            if ( other.associations != null ) { return false; }
        } else if ( !associations.equals( other.associations ) ) { return false; }
        if ( entities == null ) {
            if ( other.entities != null ) { return false; }
        } else if ( !entities.equals( other.entities ) ) { return false; }
        if ( tickets == null ) {
            if ( other.tickets != null ) { return false; }
        } else if ( !tickets.equals( other.tickets ) ) { return false; }
        return true;
    }

    @Override public String toString() {
        return "BulkDataCreation{" +
                "tickets=" + tickets +
                ", entities=" + entities +
                ", associations=" + associations +
                '}';
    }

    private static void serialize( ObjectOutputStream oos, UUID id ) throws IOException {
        oos.writeLong( id.getLeastSignificantBits() );
        oos.writeLong( id.getMostSignificantBits() );
    }

    private static void serialize( ObjectOutputStream oos, EntityKey ek ) throws IOException {
        serialize( oos, ek.getEntitySetId() );
        oos.writeUTF( ek.getEntityId() );
        serialize( oos, ek.getSyncId() );
    }

    private static UUID deserializeUUID( ObjectInputStream ois ) throws IOException {
        long lsb = ois.readLong();
        long msb = ois.readLong();
        return new UUID( msb, lsb );
    }

    private static SetMultimap<UUID, Object> deserializeEntityDetails( ObjectInputStream ois ) throws IOException {
        //        Input input = new Input( ois );
        //        return (SetMultimap<UUID, Object>) kryoThreadLocal.get().readClassAndObject( input );
        int detailCount = ois.readInt();
        SetMultimap<UUID, Object> details = HashMultimap.create();
        for ( int i = 0; i < detailCount; ++i ) {
            UUID propertyId = deserializeUUID( ois );
            Object detail = null;
            try {
                detail = ois.readObject();
            } catch ( ClassNotFoundException e ) {
                throw new IOException( "Unable to locate class", e );
            }
            details.put( propertyId, detail );
        }
        return details;
    }

    private static void serialize( ObjectOutputStream oos, SetMultimap<UUID, Object> details ) throws IOException {
        oos.writeInt( details.size() );
        for ( Entry<UUID, Object> entry : details.entries() ) {
            serialize( oos, entry.getKey() );
            oos.writeObject( entry.getValue() );
        }
    }

    private static EntityKey deserializeEntityKey( ObjectInputStream ois ) throws IOException {
        UUID entitySetId = deserializeUUID( ois );
        String entityId = ois.readUTF();
        UUID syncId = deserializeUUID( ois );
        return new EntityKey( entitySetId, entityId, syncId );
    }
}
