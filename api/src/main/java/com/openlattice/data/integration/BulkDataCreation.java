/*
 * Copyright (C) 2018. OpenLattice, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@openlattice.com
 *
 *
 */

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

package com.openlattice.data.integration;

import com.dataloom.mappers.ObjectMappers;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.openlattice.client.serialization.SerializationConstants;
import com.openlattice.data.EntityKey;
import com.openlattice.data.serializers.FullQualifiedNameJacksonSerializer;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

@SuppressFBWarnings(value="SECOBDES", justification = "Java serialization for this class only occurs client side.")
public class BulkDataCreation implements Serializable {
    static {
        FullQualifiedNameJacksonSerializer.registerWithMapper( ObjectMappers.getJsonMapper() );
    }

    private Set<Entity>      entities;
    private Set<Association> associations;

    @JsonCreator
    public BulkDataCreation(
            @JsonProperty( SerializationConstants.ENTITIES ) Set<Entity> entities,
            @JsonProperty( SerializationConstants.ASSOCIATIONS ) Set<Association> associations ) {
        this.entities = entities;
        this.associations = associations;
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

    @Override public boolean equals( Object o ) {
        if ( this == o ) { return true; }
        if ( !( o instanceof BulkDataCreation ) ) { return false; }
        BulkDataCreation that = (BulkDataCreation) o;
        return Objects.equals( entities, that.entities ) &&
                Objects.equals( associations, that.associations );
    }

    @Override public int hashCode() {

        return Objects.hash( entities, associations );
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
        return new EntityKey( entitySetId, entityId );
    }
}
