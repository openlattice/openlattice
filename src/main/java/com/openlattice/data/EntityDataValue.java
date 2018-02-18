/*
 * Copyright (C) 2018. OpenLattice, Inc
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

package com.openlattice.data;

import java.time.OffsetDateTime;
import java.util.Map;
import java.util.UUID;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class EntityDataValue {
    private final EntityDataMetadata                       metadata;
    private final Map<UUID, Map<Object, PropertyMetadata>> properties;

    public EntityDataValue(
            EntityDataMetadata metadata,
            Map<UUID, Map<Object, PropertyMetadata>> properties ) {
        this.metadata = metadata;
        this.properties = properties;
    }

    public EntityDataMetadata getMetadata() {
        return metadata;
    }

    public Map<UUID, Map<Object, PropertyMetadata>> getProperties() {
        return properties;
    }

    public long getVersion() {
        return metadata.getVersion();
    }

    public OffsetDateTime getLastWrite() {
        return metadata.getLastWrite();
    }

    public OffsetDateTime getLastIndex() {
        return metadata.getLastIndex();
    }

    @Override public boolean equals( Object o ) {
        if ( this == o ) { return true; }
        if ( !( o instanceof EntityDataValue ) ) { return false; }

        EntityDataValue that = (EntityDataValue) o;

        if ( !metadata.equals( that.metadata ) ) { return false; }
        return properties.equals( that.properties );
    }

    @Override public int hashCode() {
        int result = metadata.hashCode();
        result = 31 * result + properties.hashCode();
        return result;
    }

    @Override public String toString() {
        return "EntityDataValue{" +
                "metadata=" + metadata +
                ", properties=" + properties +
                '}';
    }

    public void setLastWrite( OffsetDateTime lastWrite ) {
        metadata.setLastWrite( lastWrite );
    }

    public void incrementVersion() {
        metadata.incrementVersion();
    }
}
