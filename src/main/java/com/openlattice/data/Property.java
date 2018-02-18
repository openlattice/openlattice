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

import com.fasterxml.jackson.annotation.JsonCreator;
import java.util.Optional;
import java.util.UUID;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class Property {
    private final Optional<UUID>   entitySetId;
    private final PropertyMetadata metadata;
    private final Object           value;

    @JsonCreator
    public Property( Optional<UUID> entitySetId, PropertyMetadata metadata, Object value ) {
        this.entitySetId = entitySetId;
        this.metadata = metadata;
        this.value = value;
    }

    public Optional<UUID> getEntitySetId() {
        return entitySetId;
    }

}
