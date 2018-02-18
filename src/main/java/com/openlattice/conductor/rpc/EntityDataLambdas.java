

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

package com.openlattice.conductor.rpc;

import com.google.common.collect.SetMultimap;

import java.io.Serializable;
import java.util.UUID;
import java.util.function.Function;

public class EntityDataLambdas implements Function<ConductorElasticsearchApi, Boolean>, Serializable {
    private static final long serialVersionUID = -1071651645473672891L;

    private UUID                      entitySetId;
    private UUID                      syncId;
    private String                    entityId;
    private SetMultimap<UUID, Object> propertyValues;
    private boolean                   shouldUpdate;

    public EntityDataLambdas(
            UUID entitySetId,
            UUID syncId,
            String entityId,
            SetMultimap<UUID, Object> propertyValues,
            boolean shouldUpdate ) {
        this.entitySetId = entitySetId;
        this.syncId = syncId;
        this.entityId = entityId;
        this.propertyValues = propertyValues;
        this.shouldUpdate = shouldUpdate;
    }

    @Override
    public Boolean apply( ConductorElasticsearchApi api ) {
        return shouldUpdate ?
                api.updateEntityData( entitySetId, syncId, entityId, propertyValues ) :
                api.createEntityData( entitySetId, syncId, entityId, propertyValues );
    }

    public UUID getEntitySetId() {
        return entitySetId;
    }

    public UUID getSyncId() {
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
