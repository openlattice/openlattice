package com.openlattice.hazelcast.serializers;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import com.openlattice.edm.set.EntitySetPropertyKey;
import com.openlattice.edm.type.EntityTypePropertyKey;
import com.openlattice.hazelcast.StreamSerializerTypeIds;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.UUID;

@Component
public class EntityTypePropertyKeyStreamSerializer implements SelfRegisteringStreamSerializer<EntityTypePropertyKey> {

    @Override
    public void write( ObjectDataOutput out, EntityTypePropertyKey object ) throws IOException {
        UUIDStreamSerializer.serialize( out, object.getEntityTypeId() );
        UUIDStreamSerializer.serialize( out, object.getPropertyTypeId() );
    }

    @Override
    public EntityTypePropertyKey read( ObjectDataInput in ) throws IOException {
        UUID entityTypeId = UUIDStreamSerializer.deserialize( in );
        UUID propertyTypeId = UUIDStreamSerializer.deserialize( in );
        return new EntityTypePropertyKey( entityTypeId, propertyTypeId );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.ENTITY_TYPE_PROPERTY_KEY.ordinal();
    }

    @Override
    public void destroy() {}

    @Override
    public Class<? extends EntityTypePropertyKey> getClazz() {
        return EntityTypePropertyKey.class;
    }

}
