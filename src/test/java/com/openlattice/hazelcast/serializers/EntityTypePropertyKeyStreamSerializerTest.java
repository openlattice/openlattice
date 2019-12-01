package com.openlattice.hazelcast.serializers;

import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest;
import com.openlattice.edm.type.EntityTypePropertyKey;

import java.io.Serializable;
import java.util.UUID;


public class EntityTypePropertyKeyStreamSerializerTest extends AbstractStreamSerializerTest<EntityTypePropertyKeyStreamSerializer, EntityTypePropertyKey>
        implements Serializable {
private static final long serialVersionUID = -4933403371497497344L;

@Override
protected EntityTypePropertyKeyStreamSerializer createSerializer() {
        return new EntityTypePropertyKeyStreamSerializer();
        }

@Override
protected EntityTypePropertyKey createInput() {
        return new EntityTypePropertyKey( UUID.randomUUID(), UUID.randomUUID() );
        }

        }
