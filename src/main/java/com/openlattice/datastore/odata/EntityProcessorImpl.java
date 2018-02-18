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

package com.openlattice.datastore.odata;

import com.openlattice.data.DatasourceManager;
import com.openlattice.datastore.DatastoreUtil;
import com.openlattice.datastore.services.ODataStorageService;
import java.io.InputStream;
import java.util.List;
import org.apache.olingo.commons.api.data.ContextURL;
import org.apache.olingo.commons.api.data.Entity;
import org.apache.olingo.commons.api.edm.EdmEntitySet;
import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.apache.olingo.commons.api.format.ContentType;
import org.apache.olingo.commons.api.http.HttpHeader;
import org.apache.olingo.commons.api.http.HttpStatusCode;
import org.apache.olingo.server.api.OData;
import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.ODataLibraryException;
import org.apache.olingo.server.api.ODataRequest;
import org.apache.olingo.server.api.ODataResponse;
import org.apache.olingo.server.api.ServiceMetadata;
import org.apache.olingo.server.api.processor.EntityProcessor;
import org.apache.olingo.server.api.serializer.EntitySerializerOptions;
import org.apache.olingo.server.api.serializer.ODataSerializer;
import org.apache.olingo.server.api.serializer.SerializerResult;
import org.apache.olingo.server.api.uri.UriInfo;
import org.apache.olingo.server.api.uri.UriParameter;
import org.apache.olingo.server.api.uri.UriResourceEntitySet;

public class EntityProcessorImpl implements EntityProcessor {
    private final ODataStorageService storage;
    private final DatasourceManager   dsm;

    private OData           odata;
    private ServiceMetadata serviceMetadata;

    public EntityProcessorImpl( ODataStorageService storage, DatasourceManager dsm ) {
        this.storage = storage;
        this.dsm = dsm;
    }

    @Override
    public void init( OData odata, ServiceMetadata serviceMetadata ) {
        this.odata = odata;
        this.serviceMetadata = serviceMetadata;
    }

    @Override
    public void readEntity( ODataRequest request, ODataResponse response, UriInfo uriInfo, ContentType responseFormat )
            throws ODataApplicationException, ODataLibraryException {
        // 1. retrieve the Entity Type
        EdmEntitySet edmEntitySet = DatastoreUtil.getEdmEntitySet( uriInfo );

        // 2. retrieve the data from backend
        List<UriParameter> keyPredicates = ( (UriResourceEntitySet) uriInfo ).getKeyPredicates();
        Entity entity = storage.readEntityData( edmEntitySet, keyPredicates );

        // 3. serialize
        EdmEntityType entityType = edmEntitySet.getEntityType();

        ContextURL contextUrl = ContextURL.with().entitySet( edmEntitySet ).build();
        // expand and select currently not supported
        EntitySerializerOptions options = EntitySerializerOptions.with().contextURL( contextUrl ).build();

        ODataSerializer serializer = odata.createSerializer( responseFormat );
        SerializerResult serializerResult = serializer.entity( serviceMetadata, entityType, entity, options );
        InputStream entityStream = serializerResult.getContent();

        // 4. configure the response object
        response.setContent( entityStream );
        response.setStatusCode( HttpStatusCode.OK.getStatusCode() );
        response.setHeader( HttpHeader.CONTENT_TYPE, responseFormat.toContentTypeString() );

    }

    @Override
    public void createEntity(
            ODataRequest request,
            ODataResponse response,
            UriInfo uriInfo,
            ContentType requestFormat,
            ContentType responseFormat ) throws ODataApplicationException, ODataLibraryException {
        /**
         // 1. Retrieve the entity type from the URI
         EdmEntitySet edmEntitySet = DatastoreUtil.getEdmEntitySet( uriInfo );
         EdmEntityType edmEntityType = edmEntitySet.getEntityType();

         // 2. create the data in backend
         // 2.1. retrieve the payload from the POST request for the entity to create and deserialize it
         InputStream requestInputStream = request.getBody();
         ODataDeserializer deserializer = this.odata.createDeserializer( requestFormat );
         DeserializerResult result = deserializer.entity( requestInputStream, edmEntityType );

         Entity requestEntity = result.getEntity();

         // 2.2 do the creation in backend, which returns the newly created entity
         Entity createdEntity = storage.createEntityData(
         ACLs.EVERYONE_ACL,
         Syncs.BASE.getSyncId(),
         edmEntitySet,
         requestEntity );

         // 3. serialize the response (we have to return the created entity)
         ContextURL contextUrl = ContextURL.with().entitySet( edmEntitySet ).build();
         // expand and select currently not supported
         EntitySerializerOptions options = EntitySerializerOptions.with().contextURL( contextUrl ).build();

         ODataSerializer serializer = this.odata.createSerializer( responseFormat );
         SerializerResult serializedResponse = serializer.entity( serviceMetadata,
         edmEntityType,
         createdEntity,
         options );

         // 4. configure the response object
         response.setContent( serializedResponse.getContent() );
         response.setStatusCode( HttpStatusCode.CREATED.getStatusCode() );
         response.setHeader( HttpHeader.CONTENT_TYPE, responseFormat.toContentTypeString() );
         */
    }

    @Override
    public void updateEntity(
            ODataRequest request,
            ODataResponse response,
            UriInfo uriInfo,
            ContentType requestFormat,
            ContentType responseFormat ) throws ODataApplicationException, ODataLibraryException {
        // TODO Auto-generated method stub

    }

    @Override
    public void deleteEntity( ODataRequest request, ODataResponse response, UriInfo uriInfo )
            throws ODataApplicationException, ODataLibraryException {
        // TODO Auto-generated method stub

    }

}
