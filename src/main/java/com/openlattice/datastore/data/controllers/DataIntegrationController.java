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

package com.openlattice.datastore.data.controllers;

import com.codahale.metrics.annotation.Timed;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.openlattice.authorization.AclKey;
import com.openlattice.authorization.AuthorizationManager;
import com.openlattice.authorization.AuthorizingComponent;
import com.openlattice.authorization.EdmAuthorizationHelper;
import com.openlattice.data.DataGraphManager;
import com.openlattice.data.DataIntegrationApi;
import com.openlattice.data.EntityKey;
import com.openlattice.data.integration.S3EntityData;
import com.openlattice.data.storage.aws.AwsDataSinkService;
import com.openlattice.datastore.services.EdmService;
import com.openlattice.edm.type.PropertyType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.inject.Inject;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.openlattice.authorization.EdmAuthorizationHelper.WRITE_PERMISSION;
import static com.openlattice.authorization.EdmAuthorizationHelper.aclKeysForAccessCheck;

@RestController
@RequestMapping( DataIntegrationApi.CONTROLLER )
public class DataIntegrationController implements DataIntegrationApi, AuthorizingComponent {
    @Inject
    private EdmService dms;

    @Inject
    private DataGraphManager dgm;

    @Inject
    private AwsDataSinkService awsDataSinkService;

    @Inject
    private AuthorizationManager authz;

    @Inject
    private EdmAuthorizationHelper authzHelper;

    @Override
    public AuthorizationManager getAuthorizationManager() {
        return authz;
    }

    @Override
    public List<String> generatePresignedUrls( Collection<S3EntityData> data ) {
        throw new UnsupportedOperationException( "This shouldn't be invoked. Just here for the interface and efficiency" );
    }

    @Timed
    @PostMapping( "/" + S3 )
    public List<String> generatePresignedUrls(
            @RequestBody List<S3EntityData> data ) {
        final Set<UUID> entitySetIds = data.stream().map( S3EntityData::getEntitySetId ).collect(
                Collectors.toSet() );
        final SetMultimap<UUID, UUID> propertyIdsByEntitySet = HashMultimap.create();
        data.forEach( entity -> propertyIdsByEntitySet
                .put( entity.getEntitySetId(), entity.getPropertyTypeId() ) );

        //Ensure that we have read access to entity set metadata.
        entitySetIds.forEach( entitySetId -> ensureReadAccess( new AclKey( entitySetId ) ) );

        accessCheck( aclKeysForAccessCheck( propertyIdsByEntitySet, WRITE_PERMISSION ) );

        final Map<UUID, Map<UUID, PropertyType>> authorizedPropertyTypes =
                entitySetIds.stream()
                        .collect( Collectors.toMap( Function.identity(),
                                entitySetId -> authzHelper.getAuthorizedPropertyTypes(
                                        entitySetId, WRITE_PERMISSION ) ) );

        return awsDataSinkService.generatePresignedUrls( data, authorizedPropertyTypes );
    }

    //Just sugar to conform to API interface. While still allow efficient serialization.
    @Override
    public List<UUID> getEntityKeyIds( Set<EntityKey> entityKeys ) {
        throw new UnsupportedOperationException( "Nobody should be calling this." );
    }

    @PostMapping( "/" + ENTITY_KEY_IDS )
    @Timed
    public Set<UUID> getEntityKeyIds( @RequestBody LinkedHashSet<EntityKey> entityKeys ) {
        return dgm.getEntityKeyIds( entityKeys );
    }

}

