package com.openlattice.data.integration;

import com.dataloom.mappers.ObjectMappers;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.client.serialization.SerializationConstants;
import com.openlattice.data.EntityKey;
import com.openlattice.data.serializers.FullQualifiedNameJacksonSerializer;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.*;

@SuppressFBWarnings( value = "SECOBDES", justification = "Java serialization for this class only occurs client side." )
public class BulkDataCreation2 implements Serializable {
    static {
        FullQualifiedNameJacksonSerializer.registerWithMapper( ObjectMappers.getJsonMapper() );
    }

    private Set<Entity>      entities;
    private Set<Association> associations;
    private Map<UUID, Set<String>> entitySetIdToEntityIds;
    //map of propertyType UUID to destination

    @JsonCreator
    public BulkDataCreation2(
            @JsonProperty( SerializationConstants.ENTITIES ) Set<Entity> entities,
            @JsonProperty( SerializationConstants.ASSOCIATIONS ) Set<Association> associations,
            @JsonProperty( SerializationConstants.ENTITY_SET_ID_TO_ENTITY_ID ) Map<UUID, Set<String>> entitySetIdToEntityIds) {
        this.entities = entities;
        this.associations = associations;
        this.entitySetIdToEntityIds = entitySetIdToEntityIds;
    }

    @JsonProperty( SerializationConstants.ENTITIES )
    public Set<Entity> getEntities() {
        return entities;
    }

    @JsonProperty( SerializationConstants.ENTITY_SET_ID_TO_ENTITY_ID)
    public Map<UUID, Set<String>> getEntitySetIdToEntityIds() {
        return entitySetIdToEntityIds;
    }

    @JsonProperty( SerializationConstants.ASSOCIATIONS )
    public Set<Association> getAssociations() {
        return associations;
    }

    @Override public boolean equals( Object o ) {
        if ( this == o )
            return true;
        if ( o == null || getClass() != o.getClass() )
            return false;
        BulkDataCreation2 that = (BulkDataCreation2) o;
        return Objects.equals( entities, that.entities ) &&
                Objects.equals( associations, that.associations ) &&
                Objects.equals( entitySetIdToEntityIds, that.entitySetIdToEntityIds );
    }

    @Override public int hashCode() {
        return Objects.hash( entities, associations, entitySetIdToEntityIds );
    }

    private void writeObject( ObjectOutputStream oos )
            throws IOException {

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
        entities = new HashSet<>();
        associations = new HashSet<>();

        int entityCount = ois.readInt();
        for ( int i = 0; i < entityCount; ++i ) {
            EntityKey ek = deserializeEntityKey( ois );
            Map<UUID, Set<Object>> details = deserializeEntityDetails( ois );
            entities.add( new Entity( ek, details ) );
        }

        int associationCount = ois.readInt();
        for ( int i = 0; i < associationCount; ++i ) {
            EntityKey src = deserializeEntityKey( ois );
            EntityKey dst = deserializeEntityKey( ois );
            EntityKey key = deserializeEntityKey( ois );
            Map<UUID, Set<Object>> details = deserializeEntityDetails( ois );
            associations.add( new Association( key, src, dst, details ) );
        }
    }

    @Override public String toString() {
        return "BulkDataCreation{" +
                "entities=" + entities +
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
    }

    private static UUID deserializeUUID( ObjectInputStream ois ) throws IOException {
        long lsb = ois.readLong();
        long msb = ois.readLong();
        return new UUID( msb, lsb );
    }

    private static Map<UUID, Set<Object>> deserializeEntityDetails( ObjectInputStream ois ) throws IOException {
        //        Input input = new Input( ois );
        //        return (SetMultimap<UUID, Object>) kryoThreadLocal.get().readClassAndObject( input );
        int detailCount = ois.readInt();
        Map<UUID, Set<Object>> details = new HashMap<>( detailCount );
        for ( int i = 0; i < detailCount; ++i ) {
            UUID propertyId = deserializeUUID( ois );
            int valueCount = ois.readInt();
            Set<Object> values = new HashSet<>( valueCount );
            for ( int j = 0; j < valueCount; ++j ) {
                try {
                    values.add( ois.readObject() );
                } catch ( ClassNotFoundException e ) {
                    throw new IOException( "Unable to locate class", e );
                }
            }
            details.put( propertyId, values );
        }
        return details;
    }

    private static void serialize( ObjectOutputStream oos, Map<UUID, Set<Object>> details ) throws IOException {
        oos.writeInt( details.size() );
        for ( Map.Entry<UUID, Set<Object>> entry : details.entrySet() ) {
            serialize( oos, entry.getKey() );
            oos.writeInt( entry.getValue().size() );
            for ( Object o : entry.getValue() ) {
                oos.writeObject( o );
            }
        }
    }

    private static EntityKey deserializeEntityKey( ObjectInputStream ois ) throws IOException {
        UUID entitySetId = deserializeUUID( ois );
        String entityId = ois.readUTF();
        return new EntityKey( entitySetId, entityId );
    }
}
