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

package com.openlattice.datastore.analysis.controllers;

import static com.openlattice.postgres.DataTables.COUNT_FQN;
import static com.openlattice.postgres.DataTables.ID_FQN;

import com.google.common.collect.SetMultimap;
import com.openlattice.analysis.AnalysisApi;
import com.openlattice.analysis.requests.NeighborType;
import com.openlattice.analysis.requests.TopUtilizerDetails;
import com.openlattice.authorization.AclKey;
import com.openlattice.authorization.AuthorizationManager;
import com.openlattice.authorization.AuthorizingComponent;
import com.openlattice.authorization.EdmAuthorizationHelper;
import com.openlattice.authorization.Permission;
import com.openlattice.data.EntitySetData;
import com.openlattice.data.requests.FileType;
import com.openlattice.datastore.constants.CustomMediaType;
import com.openlattice.datastore.services.AnalysisService;
import com.openlattice.datastore.services.EdmService;
import com.openlattice.edm.type.PropertyType;
import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.inject.Inject;
import javax.servlet.http.HttpServletResponse;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping( AnalysisApi.CONTROLLER )
public class AnalysisController implements AnalysisApi, AuthorizingComponent {

    @Inject
    private AnalysisService analysisService;

    @Inject
    private EdmService edm;

    @Inject
    private EdmAuthorizationHelper authorizationsHelper;

    @Inject
    private AuthorizationManager authorizations;

    @RequestMapping(
            path = { ENTITY_SET_ID_PATH + NUM_RESULTS_PATH },
            method = RequestMethod.POST,
            produces = { MediaType.APPLICATION_JSON_VALUE, CustomMediaType.TEXT_CSV_VALUE } )
    public EntitySetData<FullQualifiedName> getTopUtilizers(
            @PathVariable( ENTITY_SET_ID ) UUID entitySetId,
            @PathVariable( NUM_RESULTS ) int numResults,
            @RequestBody List<TopUtilizerDetails> topUtilizerDetails,
            @RequestParam(
                    value = FILE_TYPE,
                    required = false ) FileType fileType,
            HttpServletResponse response ) {
        ensureReadAccess( new AclKey( entitySetId ) );
        FileType downloadType = ( fileType == null ) ? FileType.json : fileType;
        // setContentDisposition( response, entitySetId.toString(), downloadType );
        setDownloadContentType( response, downloadType );
        return getTopUtilizers( entitySetId, numResults, topUtilizerDetails, downloadType );
    }

    @Override
    public EntitySetData<FullQualifiedName> getTopUtilizers(
            UUID entitySetId,
            int numResults,
            List<TopUtilizerDetails> topUtilizerDetails,
            FileType fileType ) {

        if ( topUtilizerDetails.size() == 0 ) { return null; }

        final Map<UUID, PropertyType> authorizedPropertyTypes = authorizationsHelper
                .getAuthorizedPropertyTypes( entitySetId, EnumSet.of( Permission.READ ) );
        //TODO: Need to actually return authorization error
        if ( authorizedPropertyTypes.size() == 0 ) { return null; }

        final Stream<SetMultimap<FullQualifiedName, Object>> utilizers = analysisService.getTopUtilizers( entitySetId,
                numResults,
                topUtilizerDetails,
                authorizedPropertyTypes );

        final LinkedHashSet<String> columnTitles = authorizedPropertyTypes.values().stream().map( pt -> pt.getType() )
                .map( fqn -> fqn.toString() )
                .collect( Collectors.toCollection( () -> new LinkedHashSet<>() ) );
        columnTitles.add( COUNT_FQN.getFullQualifiedNameAsString() );
        columnTitles.add( ID_FQN.getFullQualifiedNameAsString() );
        return new EntitySetData<>( columnTitles, utilizers::iterator );
    }

    @RequestMapping(
            path = { ENTITY_SET_ID_PATH + TYPES_PATH },
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE )
    @Override
    public Iterable<NeighborType> getNeighborTypes( @PathVariable( ENTITY_SET_ID ) UUID entitySetId ) {
        ensureReadAccess( new AclKey( entitySetId ) );
        return analysisService.getNeighborTypes( entitySetId );
    }

    @Override public AuthorizationManager getAuthorizationManager() {
        return authorizations;
    }

    private static void setDownloadContentType( HttpServletResponse response, FileType fileType ) {
        if ( fileType == FileType.csv ) {
            response.setContentType( CustomMediaType.TEXT_CSV_VALUE );
        } else {
            response.setContentType( MediaType.APPLICATION_JSON_VALUE );
        }
    }
}
