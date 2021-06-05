package com.openlattice.hazelcast.serializers;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import com.openlattice.hazelcast.StreamSerializerTypeIds;
import com.openlattice.search.PersistentSearchMessengerTask;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class PersistentSearchMessengerStreamSerializer
        implements SelfRegisteringStreamSerializer<PersistentSearchMessengerTask> {

    @Override public Class<? extends PersistentSearchMessengerTask> getClazz() {
        return PersistentSearchMessengerTask.class;
    }

    @Override public void write( ObjectDataOutput out, PersistentSearchMessengerTask object ) throws IOException {

    }

    @Override public PersistentSearchMessengerTask read( ObjectDataInput in ) throws IOException {
        return new PersistentSearchMessengerTask();
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.PERSISTENT_SEARCH_MESSENGER.ordinal();
    }

    @Override public void destroy() {

    }
}
