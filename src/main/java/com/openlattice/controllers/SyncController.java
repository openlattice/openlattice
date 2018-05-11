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

package com.openlattice.controllers;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.openlattice.authorization.AclKey;
import com.openlattice.authorization.AuthorizationManager;
import com.openlattice.authorization.AuthorizingComponent;
import com.openlattice.authorization.ForbiddenException;
import com.openlattice.authorization.Permission;
import com.openlattice.authorization.Principals;
import com.openlattice.data.DatasourceManager;
import com.openlattice.data.EntityDataKey;
import com.openlattice.data.EntityDataValue;
import com.openlattice.hazelcast.HazelcastMap;
import com.openlattice.hazelcast.predicates.EntitySetPredicates;
import com.openlattice.hazelcast.processors.SyncFinalizer;
import com.openlattice.sync.SyncApi;
import java.time.OffsetDateTime;
import java.util.EnumSet;
import java.util.UUID;
import javax.inject.Inject;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping( SyncApi.CONTROLLER )
public class SyncController implements SyncApi, AuthorizingComponent {

    private IMap<EntityDataKey, EntityDataValue> entities;
    
    @Inject
    private AuthorizationManager authz;
    @Inject
    private DatasourceManager    datasourceManager;

    @Inject
    public void setHazelcastInstance( HazelcastInstance hazelcastInstance ) {
        entities = hazelcastInstance.getMap( HazelcastMap.ENTITY_DATA.name() );
    }

    @Override
    @RequestMapping(
            path = { ENTITY_SET_ID_PATH },
            method = RequestMethod.GET )
    public UUID acquireSyncId( @PathVariable UUID entitySetId ) {
        if ( authz.checkIfHasPermissions( new AclKey( entitySetId ),
                Principals.getCurrentPrincipals(),
                EnumSet.of( Permission.WRITE ) ) ) {
            entities.executeOnEntries( new SyncFinalizer( OffsetDateTime.now() ),
                    EntitySetPredicates.entitySet( entitySetId ) );
            //            return datasourceManager.createNewSyncIdForEntitySet( entitySetId );
            return UUID.randomUUID();
        } else {
            throw new ForbiddenException(
                    "Insufficient permissions to acquire a sync id for this entity set or it doesn't exists." );
        }
    }

    @Override
    @RequestMapping(
            path = { ENTITY_SET_ID_PATH + CURRENT },
            method = RequestMethod.GET )
    public UUID getCurrentSyncId( @PathVariable UUID entitySetId ) {
        if ( authz.checkIfHasPermissions( new AclKey( entitySetId ),
                Principals.getCurrentPrincipals(),
                EnumSet.of( Permission.READ ) ) ) {
            return datasourceManager.getCurrentSyncId( entitySetId );
        } else {
            throw new ForbiddenException(
                    "Insufficient permissions to read the sync id of the entity set or it doesn't exists." );
        }
    }

    @Override
    @RequestMapping(
            path = { ENTITY_SET_ID_PATH + SYNC_ID_PATH },
            method = RequestMethod.POST )
    public Void setCurrentSyncId( @PathVariable UUID entitySetId, @PathVariable UUID syncId ) {
        if ( authz.checkIfHasPermissions( new AclKey( entitySetId ),
                Principals.getCurrentPrincipals(),
                EnumSet.of( Permission.WRITE ) ) ) {
            datasourceManager.setCurrentSyncId( entitySetId, syncId );
            return null;
        } else {
            throw new ForbiddenException(
                    "Insufficient permissions to set the current sync id of the entity set or it doesn't exists." );
        }
    }

    @Override
    @RequestMapping(
            path = { ENTITY_SET_ID_PATH + LATEST },
            method = RequestMethod.GET )
    public UUID getLatestSyncId( @PathVariable UUID entitySetId ) {
        if ( authz.checkIfHasPermissions( new AclKey( entitySetId ),
                Principals.getCurrentPrincipals(),
                EnumSet.of( Permission.READ ) ) ) {
            return datasourceManager.getLatestSyncId( entitySetId );
        } else {
            throw new ForbiddenException(
                    "Insufficient permissions to read the latest sync id of the entity set or it doesn't exists." );
        }
    }

    @Override
    public AuthorizationManager getAuthorizationManager() {
        return authz;
    }
}