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

package com.openlattice.data;

import java.util.UUID;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class PropertyKey {
    private final UUID   entitySetId;
    private final UUID   entityKeyId;
    private final UUID   propertyTypeId;
    private final Object value;

    public PropertyKey( UUID entitySetId, UUID entityKeyId, UUID propertyTypeId, Object value ) {
        this.entitySetId = entitySetId;
        this.entityKeyId = entityKeyId;
        this.propertyTypeId = propertyTypeId;
        this.value = value;
    }

    public UUID getEntitySetId() {
        return entitySetId;
    }

    public UUID getEntityKeyId() {
        return entityKeyId;
    }

    public UUID getPropertyTypeId() {
        return propertyTypeId;
    }

    public Object getValue() {
        return value;
    }
}
