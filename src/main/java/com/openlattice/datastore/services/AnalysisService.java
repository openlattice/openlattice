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

package com.openlattice.datastore.services;

import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.openlattice.analysis.requests.NeighborType;
import com.openlattice.analysis.requests.TopUtilizerDetails;
import com.openlattice.authorization.AccessCheck;
import com.openlattice.authorization.AclKey;
import com.openlattice.authorization.AuthorizationManager;
import com.openlattice.authorization.Permission;
import com.openlattice.authorization.Principals;
import com.openlattice.data.DataGraphManager;
import com.openlattice.data.DatasourceManager;
import com.openlattice.edm.EntitySet;
import com.openlattice.edm.type.EntityType;
import com.openlattice.edm.type.PropertyType;
import com.openlattice.graph.core.objects.NeighborTripletSet;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AnalysisService {

    private static final Logger logger = LoggerFactory.getLogger( AnalysisService.class );

    @Inject
    private DataGraphManager dgm;

    @Inject
    private DatasourceManager datasourceManager;

    @Inject
    private AuthorizationManager authorizations;

    @Inject
    private EdmManager edmManager;

    public Iterable<SetMultimap<Object, Object>> getTopUtilizers(
            UUID entitySetId,
            int numResults,
            List<TopUtilizerDetails> topUtilizerDetails,
            Map<UUID, PropertyType> authorizedPropertyTypes ) {
        UUID syncId = datasourceManager.getCurrentSyncId( entitySetId );
        try {
            return dgm.getTopUtilizers( entitySetId, syncId, topUtilizerDetails, numResults, authorizedPropertyTypes );
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "Unable to get top utilizer data." );
            return null;
        }
    }

    public Iterable<NeighborType> getNeighborTypes( UUID entitySetId ) {
        UUID syncId = datasourceManager.getCurrentSyncId( entitySetId );

        NeighborTripletSet neighborEntitySets = dgm.getNeighborEntitySets( entitySetId, syncId );

        Set<UUID> entitySetIds = neighborEntitySets.stream().flatMap( triplet -> triplet.stream() )
                .collect( Collectors.toSet() );

        Set<UUID> authorizedEntitySetIds = authorizations
                .accessChecksForPrincipals( neighborEntitySets.stream().flatMap( triplet -> triplet.stream() )
                                .distinct().map( id -> new AccessCheck( new AclKey( id ),
                                        EnumSet.of( Permission.READ ) ) ).collect( Collectors.toSet() ),
                        Principals.getCurrentPrincipals() )
                .filter( authorization -> authorization.getPermissions().get( Permission.READ ) )
                .map( authorization -> authorization.getAclKey().get( 0 ) ).collect(
                        Collectors.toSet() );

        Map<UUID, EntitySet> entitySets = edmManager.getEntitySetsAsMap( authorizedEntitySetIds );

        Map<UUID, EntityType> entityTypes = edmManager
                .getEntityTypesAsMap( entitySets.values().stream()
                        .map( entitySet -> entitySet.getEntityTypeId() ).collect(
                                Collectors.toSet() ) );

        Set<NeighborType> neighborTypes = Sets.newHashSet();

        neighborEntitySets.forEach( triplet -> {
            boolean src = entitySetId.equals( triplet.get( 0 ) );
            UUID associationEntitySetId = triplet.get( 1 );
            UUID neighborEntitySetId = src ? triplet.get( 2 ) : triplet.get( 0 );
            if ( authorizedEntitySetIds.contains( associationEntitySetId ) && authorizedEntitySetIds
                    .contains( neighborEntitySetId ) ) {
                neighborTypes.add( new NeighborType(
                        entityTypes.get( entitySets.get( associationEntitySetId ).getEntityTypeId() ),
                        entityTypes.get( entitySets.get( neighborEntitySetId ).getEntityTypeId() ),
                        src ) );
            }
        } );

        return neighborTypes;

    }
}
