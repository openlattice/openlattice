

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

import com.google.common.base.Optional;
import com.google.common.collect.SetMultimap;

import java.util.UUID;

public class EntityDataCreatedEvent {

    private UUID                      entitySetId;
    private Optional<UUID>            syncId;
    private String                    entityId;
    private SetMultimap<UUID, Object> propertyValues;
    private boolean                   shouldUpdate;

    public EntityDataCreatedEvent(
            UUID entitySetId,
            Optional<UUID> syncId,
            String entityId,
            SetMultimap<UUID, Object> propertyValues,
            boolean shouldUpdate ) {
        this.entitySetId = entitySetId;
        this.syncId = syncId;
        this.entityId = entityId;
        this.propertyValues = propertyValues;
        this.shouldUpdate = shouldUpdate;
    }

    public UUID getEntitySetId() {
        return entitySetId;
    }

    public Optional<UUID> getOptionalSyncId() {
        return syncId;
    }

    public String getEntityId() {
        return entityId;
    }

    public SetMultimap<UUID, Object> getPropertyValues() {
        return propertyValues;
    }

    public boolean getShouldUpdate() {
        return shouldUpdate;
    }
}
