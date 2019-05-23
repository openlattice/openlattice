package com.openlattice.edm.collection;

import com.google.common.collect.Maps;

import java.util.Map;
import java.util.Objects;
import java.util.UUID;

public class CollectionTemplates {

    private Map<UUID, Map<UUID, UUID>> templates;

    public CollectionTemplates( Map<UUID, Map<UUID, UUID>> templates ) {
        this.templates = templates;
    }

    public CollectionTemplates() {
        this( Maps.newHashMap() );
    }

    public Map<UUID, Map<UUID, UUID>> getTemplates() {
        return templates;
    }

    public synchronized void put( UUID entitySetCollectionId, UUID templateTypeId, UUID entitySetId ) {
        Map<UUID, UUID> template = templates.getOrDefault( entitySetCollectionId, Maps.newHashMap() );
        template.put( templateTypeId, entitySetId );
        templates.put( entitySetCollectionId, template );
    }

    public synchronized void putAll( Map<UUID, Map<UUID, UUID>> templateValues ) {
        for ( Map.Entry<UUID, Map<UUID, UUID>> entry : templateValues.entrySet() ) {
            Map<UUID, UUID> template = templates.getOrDefault( entry.getKey(), Maps.newHashMap() );
            template.putAll( entry.getValue() );
            templates.put( entry.getKey(), template );
        }
    }

    @Override public boolean equals( Object o ) {
        if ( this == o )
            return true;
        if ( o == null || getClass() != o.getClass() )
            return false;
        CollectionTemplates that = (CollectionTemplates) o;
        return Objects.equals( templates, that.templates );
    }

    @Override public int hashCode() {
        return Objects.hash( templates );
    }

    @Override public String toString() {
        return "CollectionTemplates{" +
                "templates=" + templates +
                '}';
    }
}
