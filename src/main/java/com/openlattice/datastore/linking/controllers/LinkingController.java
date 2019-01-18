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

package com.openlattice.datastore.linking.controllers;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Preconditions.checkNotNull;

import com.openlattice.authorization.AclKey;
import com.openlattice.authorization.AuthorizationManager;
import com.openlattice.authorization.AuthorizingComponent;
import com.openlattice.datastore.services.EdmManager;
import com.openlattice.linking.LinkingApi;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import javax.inject.Inject;

import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.springframework.web.bind.annotation.*;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@RestController
@RequestMapping( LinkingApi.CONTROLLER )
public class LinkingController implements LinkingApi, AuthorizingComponent {

    private final String PERSON_FQN = "general.person";

    @Inject
    private AuthorizationManager authorizationManager;

    @Inject
    private EdmManager edmManager;

    @Override
    public AuthorizationManager getAuthorizationManager() {
        return authorizationManager;
    }


    @Override
    @RequestMapping(
            path = SET,
            method = RequestMethod.POST)
    public Integer addEntitySetsToLinkingEntitySets( @RequestBody Map<UUID, Set<UUID>> entitySetIds ) {
        return entitySetIds.entrySet().stream().mapToInt(
                entry ->  addEntitySets(entry.getKey(), entry.getValue()) ).sum();
    }

    @Override
    @RequestMapping(
            path = SET + SET_ID_PATH,
            method = RequestMethod.PUT)
    public Integer addEntitySetsToLinkingEntitySet(
            @PathVariable( SET_ID ) UUID linkingEntitySetId,
            @RequestBody Set<UUID> entitySetIds ) {
        return addEntitySets( linkingEntitySetId, entitySetIds );
    }

    private Integer addEntitySets( UUID linkingEntitySetId,  Set<UUID> entitySetIds ) {
        ensureOwnerAccess( new AclKey( linkingEntitySetId ) );
        checkState(
                edmManager.getEntitySet( linkingEntitySetId ).isLinking(),
                "Can't add linked entity sets to a not linking entity set");
        checkLinkedEntitySets( entitySetIds );

        return edmManager.addLinkedEntitySets( linkingEntitySetId, entitySetIds );
    }

    @Override
    @RequestMapping(
            path = SET,
            method = RequestMethod.DELETE)
    public Integer removeEntitySetsFromLinkingEntitySets( @RequestBody Map<UUID, Set<UUID>> entitySetIds ) {
        return entitySetIds.entrySet().stream().mapToInt(
                entry ->  removeEntitySets(entry.getKey(), entry.getValue()) ).sum();
    }

    @Override
    @RequestMapping(
            path = SET + SET_ID_PATH,
            method = RequestMethod.DELETE)
    public Integer removeEntitySetsFromLinkingEntitySet(
            @PathVariable( SET_ID ) UUID linkingEntitySetId,
            @RequestBody Set<UUID> entitySetIds ) {
        return removeEntitySets( linkingEntitySetId, entitySetIds );
    }

    private Integer removeEntitySets( UUID linkingEntitySetId,  Set<UUID> entitySetIds ) {
        ensureOwnerAccess( new AclKey( linkingEntitySetId ) );
        checkState(
                edmManager.getEntitySet( linkingEntitySetId ).isLinking(),
                "Can't remove linked entity sets from a not linking entity set");
        checkLinkedEntitySets( entitySetIds );

        return edmManager.removeLinkedEntitySets( linkingEntitySetId, entitySetIds );
    }

    private void checkLinkedEntitySets( Set<UUID> entitySetIds) {
        checkNotNull(entitySetIds);
        checkState(!entitySetIds.isEmpty(),  "Linked entity sets is empty" );

        UUID entityTypeId = edmManager.getEntityType( new FullQualifiedName( PERSON_FQN ) ).getId();
        checkState(
                entitySetIds.stream()
                        .map( it ->  edmManager.getEntitySet(it).getEntityTypeId() )
                        .allMatch( entityTypeId::equals ),
                "Linked entity sets are of differing entity types than %s :{}",
                PERSON_FQN, entitySetIds );
    }
}
