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
 *
 */

package com.openlattice.edm.events;

import java.util.UUID;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

/**
 */
public class PropertyTypeFqnChangedEvent {
    private final UUID              propertyTypeId;
    private final FullQualifiedName current;
    private final FullQualifiedName update;

    public PropertyTypeFqnChangedEvent(
            UUID propertyTypeId,
            FullQualifiedName current,
            FullQualifiedName update ) {
        this.propertyTypeId = propertyTypeId;
        this.current = current;
        this.update = update;
    }

    public UUID getPropertyTypeId() {
        return propertyTypeId;
    }

    public FullQualifiedName getCurrent() {
        return current;
    }

    public FullQualifiedName getUpdate() {
        return update;
    }
}
