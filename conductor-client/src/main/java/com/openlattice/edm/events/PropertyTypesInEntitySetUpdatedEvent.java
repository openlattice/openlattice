

/*
 * Copyright (C) 2018. OpenLattice, Inc.
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

package com.openlattice.edm.events;

import java.util.List;
import java.util.UUID;

import com.openlattice.edm.type.PropertyType;

public class PropertyTypesInEntitySetUpdatedEvent {

    private UUID entitySetId;
    private List<PropertyType> updatedPropertyTypes;
    private Boolean isFqnUpdated;

    /**
     * @param entitySetId          The id of the entity set, which has this property type.
     * @param updatedPropertyTypes The list of {@link PropertyType}s which were updated.
     * @param isFqnUpdated         True, if the updatedPropertyTypes
     *                             {@link org.apache.olingo.commons.api.edm.FullQualifiedName} has changed.
     */
    public PropertyTypesInEntitySetUpdatedEvent(
            UUID entitySetId,
            List<PropertyType> updatedPropertyTypes,
            Boolean isFqnUpdated ) {
        this.entitySetId = entitySetId;
        this.updatedPropertyTypes = updatedPropertyTypes;
        this.isFqnUpdated = isFqnUpdated;
    }

    public UUID getEntitySetId() {
        return entitySetId;
    }

    public List<PropertyType> getUpdatedPropertyTypes() {
        return updatedPropertyTypes;
    }

    public Boolean getFqnUpdated() {
        return isFqnUpdated;
    }

}
