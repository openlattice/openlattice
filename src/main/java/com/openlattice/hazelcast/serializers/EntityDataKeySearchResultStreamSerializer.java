package com.openlattice.hazelcast.serializers;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import com.openlattice.data.EntityDataKey;
import com.openlattice.hazelcast.StreamSerializerTypeIds;
import com.openlattice.search.requests.EntityDataKeySearchResult;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

@Component
public class EntityDataKeySearchResultStreamSerializer implements
        SelfRegisteringStreamSerializer<EntityDataKeySearchResult> {
    @Override public Class<? extends EntityDataKeySearchResult> getClazz() {
        return EntityDataKeySearchResult.class;
    }

    @Override public void write(
            ObjectDataOutput out, EntityDataKeySearchResult object ) throws IOException {
        out.writeLong( object.getNumHits() );
        out.writeInt( object.getEntityDataKeys().size() );
        for ( EntityDataKey id : object.getEntityDataKeys() ) {
            EntityDataKeyStreamSerializer.serialize( out, id );
        }
    }

    @Override public EntityDataKeySearchResult read( ObjectDataInput in ) throws IOException {
        long numHits = in.readLong();
        int numIds = in.readInt();
        Set<EntityDataKey> entityDataKeys = new HashSet<>( numIds );
        for ( int i = 0; i < numIds; i++ ) {
            entityDataKeys.add( EntityDataKeyStreamSerializer.deserialize( in ) );
        }
        return new EntityDataKeySearchResult( numHits, entityDataKeys );
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.ENTITY_DATA_KEY_SEARCH_RESULT.ordinal();
    }

    @Override public void destroy() {

    }
}
