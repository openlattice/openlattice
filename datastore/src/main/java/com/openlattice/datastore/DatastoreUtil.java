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

package com.openlattice.datastore;

import java.util.List;
import java.util.Locale;
import org.apache.olingo.commons.api.data.Entity;
import org.apache.olingo.commons.api.data.EntityCollection;
import org.apache.olingo.commons.api.edm.EdmEntitySet;
import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.apache.olingo.commons.api.edm.EdmPrimitiveType;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeException;
import org.apache.olingo.commons.api.edm.EdmProperty;
import org.apache.olingo.commons.api.edm.EdmType;
import org.apache.olingo.commons.api.http.HttpStatusCode;
import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.uri.UriInfoResource;
import org.apache.olingo.server.api.uri.UriParameter;
import org.apache.olingo.server.api.uri.UriResource;
import org.apache.olingo.server.api.uri.UriResourceEntitySet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class DatastoreUtil {
    private static final Logger logger = LoggerFactory.getLogger( Datastore.class );

    private DatastoreUtil() {
    }

    public static EdmEntitySet getEdmEntitySet( UriInfoResource uriInfo ) throws ODataApplicationException {

        List<UriResource> resourcePaths = uriInfo.getUriResourceParts();
        // To get the entity set we have to interpret all URI segments
        if ( !( resourcePaths.get( 0 ) instanceof UriResourceEntitySet ) ) {
            throw new ODataApplicationException(
                    "Invalid resource type for first segment.",
                    HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(),
                    Locale.ENGLISH );
        }

        UriResourceEntitySet uriResource = (UriResourceEntitySet) resourcePaths.get( 0 );

        return uriResource.getEntitySet();
    }

    public static Entity findEntity(
            EdmEntityType edmEntityType,
            EntityCollection rt_entitySet,
            List<UriParameter> keyParams )
            throws ODataApplicationException {

        List<Entity> entityList = rt_entitySet.getEntities();

        // loop over all entities in order to find that one that matches all keys in request
        // an example could be e.g. contacts(ContactID=1, CompanyID=1)
        for ( Entity rt_entity : entityList ) {
            boolean foundEntity = entityMatchesAllKeys( edmEntityType, rt_entity, keyParams );
            if ( foundEntity ) {
                return rt_entity;
            }
        }

        return null;
    }

    public static boolean entityMatchesAllKeys(
            EdmEntityType edmEntityType,
            Entity rt_entity,
            List<UriParameter> keyParams )
            throws ODataApplicationException {

        // loop over all keys
        for ( final UriParameter key : keyParams ) {
            // key
            String keyName = key.getName();
            String keyText = key.getText();

            // Edm: we need this info for the comparison below
            EdmProperty edmKeyProperty = (EdmProperty) edmEntityType.getProperty( keyName );
            Boolean isNullable = edmKeyProperty.isNullable();
            Integer maxLength = edmKeyProperty.getMaxLength();
            Integer precision = edmKeyProperty.getPrecision();
            Boolean isUnicode = edmKeyProperty.isUnicode();
            Integer scale = edmKeyProperty.getScale();
            // get the EdmType in order to compare
            EdmType edmType = edmKeyProperty.getType();
            // Key properties must be instance of primitive type
            EdmPrimitiveType edmPrimitiveType = (EdmPrimitiveType) edmType;

            // Runtime data: the value of the current entity
            Object valueObject = rt_entity.getProperty( keyName ).getValue(); // null-check is done in FWK

            // now need to compare the valueObject with the keyText String
            // this is done using the type.valueToString //
            String valueAsString = null;
            try {
                valueAsString = edmPrimitiveType.valueToString( valueObject,
                        isNullable,
                        maxLength,
                        precision,
                        scale,
                        isUnicode );
            } catch ( EdmPrimitiveTypeException e ) {
                throw new ODataApplicationException(
                        "Failed to retrieve String value",
                        HttpStatusCode.INTERNAL_SERVER_ERROR.getStatusCode(),
                        Locale.ENGLISH,
                        e );
            }

            if ( valueAsString == null ) {
                return false;
            }

            boolean matches = valueAsString.equals( keyText );
            if ( !matches ) {
                // if any of the key properties is not found in the entity, we don't need to search further
                return false;
            }
        }

        return true;
    }

    public static <T> Iterable<T> wrapForJackson( Iterable<T> iterable ) {
        return iterable::iterator;
    }

}
