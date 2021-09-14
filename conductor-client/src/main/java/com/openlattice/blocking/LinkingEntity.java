package com.openlattice.blocking;

import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.openlattice.rhizome.hazelcast.DelegatedStringSet;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

public class LinkingEntity {
    private static final long serialVersionUID = 3577378946466844645L;

    @SuppressFBWarnings( value = "SE_BAD_FIELD", justification = "Custom Stream Serializer is implemented" )
    private final Map<UUID, DelegatedStringSet> entity;

    public LinkingEntity( Map<UUID, DelegatedStringSet> entity ) {
        this.entity = entity;
    }

    public Map<UUID, DelegatedStringSet> getEntity() {
        return entity;
    }

    public boolean containsKey( UUID id ) {
        return entity.containsKey( id );
    }

    public void addEntry( UUID id, String value ) {
        if ( entity.containsKey( id ) )
            entity.get( id ).add( value );
        else
            entity.put( id, DelegatedStringSet.wrap( Sets.newHashSet( value ) ) );
    }

    public void addEntry( UUID id, DelegatedStringSet values ) {
        if ( entity.containsKey( id ) )
            entity.get( id ).addAll( values );
        else
            entity.put( id, values );
    }

    public void addAll( SetMultimap<UUID, Object> values ) {
        values.asMap().forEach( ( key, value1 ) -> {
            DelegatedStringSet stringValues = DelegatedStringSet
                    .wrap( value1.stream().filter( value -> value != null && value.toString() != null )
                            .map( Object::toString ).collect(
                                    Collectors.toSet() ) );
            entity.put( key, stringValues );
        } );
    }

    @Override public boolean equals( Object o ) {
        if ( this == o )
            return true;
        if ( o == null || getClass() != o.getClass() )
            return false;

        LinkingEntity that = (LinkingEntity) o;

        return entity != null ? entity.equals( that.entity ) : that.entity == null;
    }

    @Override public int hashCode() {
        return entity != null ? entity.hashCode() : 0;
    }
}
