package com.openlattice.hazelcast.serializers;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import com.openlattice.hazelcast.StreamSerializerTypeIds;
import com.openlattice.search.PersistentSearchMessenger;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class PersistentSearchMessengerStreamSerializer
        implements SelfRegisteringStreamSerializer<PersistentSearchMessenger> {

    @Override public Class<? extends PersistentSearchMessenger> getClazz() {
        return PersistentSearchMessenger.class;
    }

    @Override public void write( ObjectDataOutput out, PersistentSearchMessenger object ) throws IOException {

    }

    @Override public PersistentSearchMessenger read( ObjectDataInput in ) throws IOException {
        return new PersistentSearchMessenger();
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.PERSISTENT_SEARCH_MESSENGER.ordinal();
    }

    @Override public void destroy() {

    }
}
