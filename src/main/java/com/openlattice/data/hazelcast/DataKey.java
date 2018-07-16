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

package com.openlattice.data.hazelcast;

import com.datastax.driver.core.Row;
import com.openlattice.datastore.cassandra.CommonColumns;
import com.openlattice.datastore.cassandra.RowAdapters;
import java.util.UUID;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class DataKey {
    private final UUID   id;
    private final UUID   entitySetId;
    private final String entityId;
    private final UUID   propertyTypeId;
    private final byte[] hash;

    public DataKey( UUID id, UUID entitySetId, String entityId, UUID propertyTypeId, byte[] hash ) {
        this.id = id;
        this.entitySetId = entitySetId;
        this.entityId = entityId;
        this.propertyTypeId = propertyTypeId;
        this.hash = hash.clone();
    }

    public UUID getId() {
        return id;
    }

    public UUID getEntitySetId() {
        return entitySetId;
    }

    public String getEntityId() {
        return entityId;
    }

    public UUID getPropertyTypeId() {
        return propertyTypeId;
    }

    public byte[] getHash() {
        return hash.clone();
    }

    public PropertyKey toPropertyKey() {
        return new PropertyKey( id, propertyTypeId );
    }

    public static DataKey fromRow( Row row ) {
        UUID id = RowAdapters.id( row );
        UUID entitySetId = RowAdapters.entitySetId( row );
        String entityId = RowAdapters.entityId( row );
        UUID propertyTypeId = RowAdapters.propertyTypeId( row );
        byte[] hash = row.getBytes( CommonColumns.PROPERTY_VALUE.cql() ).array();
        return new DataKey( id, entitySetId, entityId, propertyTypeId, hash );
    }
}
