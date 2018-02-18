/*
 * Copyright (C) 2017. OpenLattice, Inc
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
 */

package com.openlattice.data.storage;

import com.google.common.collect.SetMultimap;
import com.hazelcast.core.Offloadable;
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor;
import java.util.Map.Entry;
import java.util.UUID;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class EntityBytesMerger extends AbstractRhizomeEntryProcessor<UUID, EntityBytes, Object> implements Offloadable {
    private final EntityBytes entity;

    public EntityBytesMerger( EntityBytes entity ) {
        this.entity = entity;
    }

    public EntityBytes getEntity() {
        return entity;
    }

    @Override public Object process( Entry<UUID, EntityBytes> entry ) {
            EntityBytes eb = entry.getValue();
        if ( eb != null ) {
            eb.getRaw().putAll( entity.getRaw() );
        } else {
            eb = entity;
        }
        entry.setValue( eb );
        return null;
    }

    @Override public String getExecutorName() {
        return OFFLOADABLE_EXECUTOR;
    }

    @Override public boolean equals( Object o ) {
        if ( this == o ) { return true; }
        if ( !( o instanceof EntityBytesMerger ) ) { return false; }

        EntityBytesMerger that = (EntityBytesMerger) o;

        return entity != null ? entity.equals( that.entity ) : that.entity == null;
    }

    @Override public int hashCode() {
        return entity != null ? entity.hashCode() : 0;
    }

    @Override public String toString() {
        return "EntityBytesMerger{" +
                "entity=" + entity +
                '}';
    }
}

