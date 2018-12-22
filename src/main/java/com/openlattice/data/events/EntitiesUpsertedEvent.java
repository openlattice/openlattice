

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

package com.openlattice.data.events;

import com.google.common.collect.SetMultimap;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class EntitiesUpsertedEvent {

    private UUID                                entitySetId;
    private Map<UUID, Map<UUID, Set<Object>>> entities;

    public EntitiesUpsertedEvent(
            UUID entitySetId,
            Map<UUID, Map<UUID, Set<Object>>> entities ) {
        this.entitySetId = entitySetId;
        this.entities = entities;
    }

    public UUID getEntitySetId() {
        return entitySetId;
    }

    public Map<UUID, Map<UUID, Set<Object>>> getEntities() {
        return entities;
    }
}
