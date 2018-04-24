package com.openlattice.hazelcast.serializers;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import com.openlattice.hazelcast.StreamSerializerTypeIds;
import com.openlattice.search.requests.EntityKeyIdSearchResult;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Component
public class EntityKeyIdSearchResultStreamSerializer
        implements SelfRegisteringStreamSerializer<EntityKeyIdSearchResult> {
    @Override
    public Class<? extends EntityKeyIdSearchResult> getClazz() {
        return EntityKeyIdSearchResult.class;
    }

    @Override
    public void write(
            ObjectDataOutput out, EntityKeyIdSearchResult object ) throws IOException {
        out.writeLong( object.getNumHits() );
        out.writeInt( object.getEntityKeyIds().size() );
        for ( UUID id : object.getEntityKeyIds() ) {
            UUIDStreamSerializer.serialize( out, id );
        }

    }

    @Override
    public EntityKeyIdSearchResult read( ObjectDataInput in ) throws IOException {
        long numHits = in.readLong();
        int numIds = in.readInt();
        List<UUID> entityKeyIds = new ArrayList<>( numIds );
        for ( int i = 0; i < numIds; i++ ) {
            entityKeyIds.add( UUIDStreamSerializer.deserialize( in ) );
        }
        return new EntityKeyIdSearchResult( numHits, entityKeyIds );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.ENTITY_KEY_ID_SEARCH_RESULT.ordinal();
    }

    @Override
    public void destroy() {
    }
}
