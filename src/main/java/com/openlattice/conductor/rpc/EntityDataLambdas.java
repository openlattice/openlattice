

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

import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.openlattice.data.EntityDataKey;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.Serializable;
import java.util.UUID;
import java.util.function.Function;

public class EntityDataLambdas implements Function<ConductorElasticsearchApi, Boolean>, Serializable {
    private static final long serialVersionUID = -1071651645473672891L;

    @SuppressFBWarnings( value = "SE_BAD_FIELD", justification = "Custom Stream Serializer is implemented" )
    private EntityDataKey             edk;
    private SetMultimap<UUID, Object> propertyValues;
    private boolean                   shouldUpdate;

    public EntityDataLambdas(
            EntityDataKey edk,
            SetMultimap<UUID, Object> propertyValues,
            boolean shouldUpdate ) {
        this.edk = edk;
        this.propertyValues = propertyValues;
        this.shouldUpdate = shouldUpdate;
    }

    @Override
    public Boolean apply( ConductorElasticsearchApi api ) {
        return shouldUpdate ?
                api.updateEntityData( edk, Multimaps.asMap( propertyValues ) ) :
                api.createEntityData( edk, Multimaps.asMap( propertyValues ) );
    }

    public EntityDataKey getEntityDataKey() {
        return edk;
    }

    public SetMultimap<UUID, Object> getPropertyValues() {
        return propertyValues;
    }

    public boolean getShouldUpdate() {
        return shouldUpdate;
    }
}
