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

import com.openlattice.apps.App;
import com.openlattice.apps.AppType;
import com.openlattice.authorization.AclKey;
import com.openlattice.authorization.securable.SecurableObjectType;
import com.openlattice.data.EntityDataKey;
import com.openlattice.edm.EntitySet;
import com.openlattice.edm.collection.EntitySetCollection;
import com.openlattice.edm.collection.EntityTypeCollection;
import com.openlattice.edm.type.AssociationType;
import com.openlattice.edm.type.EntityType;
import com.openlattice.edm.type.PropertyType;
import com.openlattice.organization.Organization;
import com.openlattice.search.requests.SearchResult;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;

public class ElasticsearchLambdas implements Serializable {
    private static final long serialVersionUID = -4180766624983725307L;

    public static Function<ConductorElasticsearchApi, Boolean> submitEntitySetToElasticsearch(
            EntityType entityType,
            EntitySet entitySet,
            List<PropertyType> propertyTypes ) {
        return (Function<ConductorElasticsearchApi, Boolean> & Serializable) ( api ) -> api
                .saveEntitySetToElasticsearch( entityType, entitySet, propertyTypes );
    }

    public static Function<ConductorElasticsearchApi, SearchResult> executeEntitySetMetadataQuery(
            Optional<String> optionalQuery,
            Optional<UUID> optionalEntityType,
            Optional<Set<UUID>> optionalPropertyTypes,
            Set<AclKey> authorizedAclKeys,
            int start,
            int maxHits ) {
        return (Function<ConductorElasticsearchApi, SearchResult> & Serializable) ( api ) -> api
                .executeEntitySetMetadataSearch( optionalQuery,
                        optionalEntityType,
                        optionalPropertyTypes,
                        authorizedAclKeys,
                        start,
                        maxHits );
    }

    public static Function<ConductorElasticsearchApi, Boolean> deleteEntitySet( UUID entitySetId, UUID entityTypeId ) {
        return (Function<ConductorElasticsearchApi, Boolean> & Serializable) ( api ) -> api
                .deleteEntitySet( entitySetId, entityTypeId );
    }

    public static Function<ConductorElasticsearchApi, Boolean> clearEntitySetData(
            UUID entitySetId,
            UUID entityTypeId ) {
        return (Function<ConductorElasticsearchApi, Boolean> & Serializable) ( api ) -> api
                .clearEntitySetData( entitySetId, entityTypeId );
    }

    public static Function<ConductorElasticsearchApi, Boolean> createOrganization( Organization organization ) {
        return (Function<ConductorElasticsearchApi, Boolean> & Serializable) ( api ) -> api
                .createOrganization( organization );
    }

    public static Function<ConductorElasticsearchApi, SearchResult> executeOrganizationKeywordSearch(
            String searchTerm,
            Set<AclKey> authorizedOrganizationIds,
            int start,
            int maxHits ) {
        return (Function<ConductorElasticsearchApi, SearchResult> & Serializable) ( api ) -> api
                .executeOrganizationSearch( searchTerm, authorizedOrganizationIds, start, maxHits );
    }

    public static Function<ConductorElasticsearchApi, Boolean> updateOrganization(
            UUID id,
            Optional<String> optionalTitle,
            Optional<String> optionalDescription ) {
        return (Function<ConductorElasticsearchApi, Boolean> & Serializable) ( api ) -> api
                .updateOrganization( id, optionalTitle, optionalDescription );
    }

    public static Function<ConductorElasticsearchApi, Boolean> deleteOrganization( UUID organizationId ) {
        return (Function<ConductorElasticsearchApi, Boolean> & Serializable) ( api ) -> api
                .deleteOrganization( organizationId );
    }

    public static Function<ConductorElasticsearchApi, Boolean> updateEntitySetMetadata( EntitySet entitySet ) {
        return (Function<ConductorElasticsearchApi, Boolean> & Serializable) ( api ) -> api
                .updateEntitySetMetadata( entitySet );
    }

    public static Function<ConductorElasticsearchApi, Boolean> updatePropertyTypesInEntitySet(
            UUID entitySetId,
            List<PropertyType> newPropertyTypes ) {
        return (Function<ConductorElasticsearchApi, Boolean> & Serializable) ( api ) -> api
                .updatePropertyTypesInEntitySet( entitySetId, newPropertyTypes );
    }

    public static Function<ConductorElasticsearchApi, Boolean> addPropertyTypesToEntityType(
            EntityType entityType,
            List<PropertyType> newPropertyTypes ) {
        return (Function<ConductorElasticsearchApi, Boolean> & Serializable) ( api ) -> api
                .addPropertyTypesToEntityType( entityType, newPropertyTypes );
    }

    public static Function<ConductorElasticsearchApi, Boolean> saveEntityTypeToElasticsearch(
            EntityType entityType,
            List<PropertyType> propertyTypes ) {
        return (Function<ConductorElasticsearchApi, Boolean> & Serializable) ( api ) -> api
                .saveEntityTypeToElasticsearch( entityType, propertyTypes );
    }

    public static Function<ConductorElasticsearchApi, Boolean> saveAssociationTypeToElasticsearch(
            AssociationType associationType,
            List<PropertyType> propertyTypes ) {
        return (Function<ConductorElasticsearchApi, Boolean> & Serializable) ( api ) -> api
                .saveAssociationTypeToElasticsearch( associationType, propertyTypes );
    }

    public static Function<ConductorElasticsearchApi, Boolean> saveSecurableObjectToElasticsearch(
            SecurableObjectType securableObjectType,
            Object securableObject ) {
        return (Function<ConductorElasticsearchApi, Boolean> & Serializable) ( api ) -> api
                .saveSecurableObjectToElasticsearch( securableObjectType, securableObject );
    }

    public static Function<ConductorElasticsearchApi, Boolean> deleteSecurableObject(
            SecurableObjectType securableObjectType,
            UUID securableObjectId ) {
        return (Function<ConductorElasticsearchApi, Boolean> & Serializable) ( api ) -> api
                .deleteSecurableObjectFromElasticsearch( securableObjectType, securableObjectId );
    }

    public static Function<ConductorElasticsearchApi, SearchResult> executeEntitySetCollectionSearch(
            String searchTerm,
            Set<AclKey> authorizedEntitySetCollectionIds,
            int start,
            int maxHits ) {
        return (Function<ConductorElasticsearchApi, SearchResult> & Serializable) ( api ) -> api
                .executeEntitySetCollectionSearch( searchTerm, authorizedEntitySetCollectionIds, start, maxHits );
    }

    public static Function<ConductorElasticsearchApi, SearchResult> executeSecurableObjectSearch(
            SecurableObjectType securableObjectType,
            String searchTerm,
            int start,
            int maxHits ) {
        return (Function<ConductorElasticsearchApi, SearchResult> & Serializable) ( api ) -> api
                .executeSecurableObjectSearch( securableObjectType, searchTerm, start, maxHits );
    }

    public static Function<ConductorElasticsearchApi, SearchResult> executeSecurableObjectFQNSearch(
            SecurableObjectType securableObjectType,
            String namespace,
            String name,
            int start,
            int maxHits ) {
        return (Function<ConductorElasticsearchApi, SearchResult> & Serializable) ( api ) -> api
                .executeSecurableObjectFQNSearch( securableObjectType, namespace, name, start, maxHits );
    }

    public static Function<ConductorElasticsearchApi, Boolean> deleteEntityData(
            EntityDataKey edk,
            UUID entityTypeId ) {
        return (Function<ConductorElasticsearchApi, Boolean> & Serializable) ( api ) -> api
                .deleteEntityData( edk, entityTypeId );
    }

    public static Function<ConductorElasticsearchApi, Boolean> deleteEntityDataBulk(
            UUID entitySetId,
            UUID entityTypeId,
            Set<UUID> entityKeyIds ) {
        return (Function<ConductorElasticsearchApi, Boolean> & Serializable) ( api ) -> api
                .deleteEntityDataBulk( entitySetId, entityTypeId, entityKeyIds );
    }

    public static Function<ConductorElasticsearchApi, Boolean> clearAllData() {
        return (Function<ConductorElasticsearchApi, Boolean> & Serializable) ( api ) -> api
                .clearAllData();
    }

    public static Function<ConductorElasticsearchApi, Boolean> triggerSecurableObjectIndex(
            SecurableObjectType securableObjectType,
            Iterable<?> securableObjects ) {
        return (Function<ConductorElasticsearchApi, Boolean> & Serializable) ( api ) -> api
                .triggerSecurableObjectIndex( securableObjectType, securableObjects );
    }

    public static Function<ConductorElasticsearchApi, Boolean> triggerEntitySetIndex(
            Map<EntitySet, Set<UUID>> entitySets,
            Map<UUID, PropertyType> propertyTypes ) {
        return (Function<ConductorElasticsearchApi, Boolean> & Serializable) ( api ) -> api
                .triggerEntitySetIndex( entitySets, propertyTypes );
    }

    public static Function<ConductorElasticsearchApi, Boolean> triggerOrganizationIndex( List<Organization> organizations ) {
        return (Function<ConductorElasticsearchApi, Boolean> & Serializable) ( api ) -> api
                .triggerOrganizationIndex( organizations );
    }

    public static <T> Function<ConductorElasticsearchApi, Set<UUID>> getEntitySetsWithIndices() {
        return (Function<ConductorElasticsearchApi, Set<UUID>> & Serializable) ConductorElasticsearchApi::getEntityTypesWithIndices;
    }
}
