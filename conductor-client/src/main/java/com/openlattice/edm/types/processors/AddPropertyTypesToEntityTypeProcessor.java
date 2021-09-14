

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

package com.openlattice.edm.types.processors;

import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import com.openlattice.edm.type.EntityType;
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor;

public class AddPropertyTypesToEntityTypeProcessor extends AbstractRhizomeEntryProcessor<UUID, EntityType, Object> {
    private static final long serialVersionUID = -8192830001484436836L;
    private final Set<UUID>   propertyTypeIds;

    public AddPropertyTypesToEntityTypeProcessor( Set<UUID> propertyTypeIds ) {
        this.propertyTypeIds = propertyTypeIds;
    }

    @Override
    public Object process( Entry<UUID, EntityType> entry ) {
        EntityType et = entry.getValue();
        if ( et != null ) {
            et.addPropertyTypes( propertyTypeIds );
            entry.setValue( et );
        }
        return null;
    }

    public Set<UUID> getPropertyTypeIds() {
        return propertyTypeIds;
    }
}
