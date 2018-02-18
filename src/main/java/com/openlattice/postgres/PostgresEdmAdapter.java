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

package com.openlattice.postgres;

import com.openlattice.authorization.securable.SecurableObjectType;
import com.openlattice.datastore.services.EdmService;
import java.util.List;
import java.util.UUID;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class PostgresEdmAdapter {
    private final EdmService edmService;

    public PostgresEdmAdapter( EdmService edmService ) {
        this.edmService = edmService;
    }

    /**
     * Creates a entity set table for holding entities of a specific type
     * @param entitySetId
     */
    public void createEntitySetTable( UUID entitySetId ) {

    }

    public void createPropertyTypeTableForEntitySet(
            UUID entitySetId,
            UUID propertyTypeId,
            PostgresDatatype datatype ) {

    }

    public void createSecurableObject( List<UUID> aclKey, SecurableObjectType securableObjectType ) {

    }
}
