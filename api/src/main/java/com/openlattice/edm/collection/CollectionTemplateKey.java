package com.openlattice.edm.collection;

import java.util.Objects;
import java.util.UUID;

public class CollectionTemplateKey {

    private UUID entitySetCollectionId;
    private UUID templateTypeId;

    public CollectionTemplateKey( UUID entitySetCollectionId, UUID templateTypeId ) {
        this.entitySetCollectionId = entitySetCollectionId;
        this.templateTypeId = templateTypeId;
    }

    public UUID getEntitySetCollectionId() {
        return entitySetCollectionId;
    }

    public UUID getTemplateTypeId() {
        return templateTypeId;
    }

    @Override public boolean equals( Object o ) {
        if ( this == o )
            return true;
        if ( o == null || getClass() != o.getClass() )
            return false;
        CollectionTemplateKey that = (CollectionTemplateKey) o;
        return Objects.equals( entitySetCollectionId, that.entitySetCollectionId ) &&
                Objects.equals( templateTypeId, that.templateTypeId );
    }

    @Override public int hashCode() {
        return Objects.hash( entitySetCollectionId, templateTypeId );
    }

    @Override public String toString() {
        return "CollectionTemplateKey{" +
                "entitySetCollectionId=" + entitySetCollectionId +
                ", templateTypeId=" + templateTypeId +
                '}';
    }
}
