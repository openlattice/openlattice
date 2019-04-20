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
import com.openlattice.data.EntityDataKey;
import com.openlattice.edm.EntitySet;
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

    public static Function<ConductorElasticsearchApi, Boolean> savePropertyTypeToElasticsearch( PropertyType propertyType ) {
        return (Function<ConductorElasticsearchApi, Boolean> & Serializable) ( api ) -> api
                .savePropertyTypeToElasticsearch( propertyType );
    }

    public static Function<ConductorElasticsearchApi, Boolean> saveAppToElasticsearch( App app ) {
        return (Function<ConductorElasticsearchApi, Boolean> & Serializable) ( api ) -> api
                .saveAppToElasticsearch( app );
    }

    public static Function<ConductorElasticsearchApi, Boolean> saveAppTypeToElasticsearch( AppType appType ) {
        return (Function<ConductorElasticsearchApi, Boolean> & Serializable) ( api ) -> api
                .saveAppTypeToElasticsearch( appType );
    }

    public static Function<ConductorElasticsearchApi, Boolean> deleteEntityType( UUID entityTypeId ) {
        return (Function<ConductorElasticsearchApi, Boolean> & Serializable) ( api ) -> api
                .deleteEntityType( entityTypeId );
    }

    public static Function<ConductorElasticsearchApi, Boolean> deleteAssociationType( UUID associationTypeId ) {
        return (Function<ConductorElasticsearchApi, Boolean> & Serializable) ( api ) -> api
                .deleteAssociationType( associationTypeId );
    }

    public static Function<ConductorElasticsearchApi, Boolean> deletePropertyType( UUID propertyTypeId ) {
        return (Function<ConductorElasticsearchApi, Boolean> & Serializable) ( api ) -> api
                .deletePropertyType( propertyTypeId );
    }

    public static Function<ConductorElasticsearchApi, Boolean> deleteApp( UUID appId ) {
        return (Function<ConductorElasticsearchApi, Boolean> & Serializable) ( api ) -> api
                .deleteApp( appId );
    }

    public static Function<ConductorElasticsearchApi, Boolean> deleteAppType( UUID appTypeId ) {
        return (Function<ConductorElasticsearchApi, Boolean> & Serializable) ( api ) -> api
                .deleteAppType( appTypeId );
    }

    public static Function<ConductorElasticsearchApi, SearchResult> executeEntityTypeSearch(
            String searchTerm,
            int start,
            int maxHits ) {
        return (Function<ConductorElasticsearchApi, SearchResult> & Serializable) ( api ) -> api
                .executeEntityTypeSearch( searchTerm, start, maxHits );
    }

    public static Function<ConductorElasticsearchApi, SearchResult> executeAssociationTypeSearch(
            String searchTerm,
            int start,
            int maxHits ) {
        return (Function<ConductorElasticsearchApi, SearchResult> & Serializable) ( api ) -> api
                .executeAssociationTypeSearch( searchTerm, start, maxHits );
    }

    public static Function<ConductorElasticsearchApi, SearchResult> executeAppSearch(
            String searchTerm,
            int start,
            int maxHits ) {
        return (Function<ConductorElasticsearchApi, SearchResult> & Serializable) ( api ) -> api
                .executeAppSearch( searchTerm, start, maxHits );
    }

    public static Function<ConductorElasticsearchApi, SearchResult> executeAppTypeSearch(
            String searchTerm,
            int start,
            int maxHits ) {
        return (Function<ConductorElasticsearchApi, SearchResult> & Serializable) ( api ) -> api
                .executeAppTypeSearch( searchTerm, start, maxHits );
    }

    public static Function<ConductorElasticsearchApi, SearchResult> executePropertyTypeSearch(
            String searchTerm,
            int start,
            int maxHits ) {
        return (Function<ConductorElasticsearchApi, SearchResult> & Serializable) ( api ) -> api
                .executePropertyTypeSearch( searchTerm, start, maxHits );
    }

    public static Function<ConductorElasticsearchApi, SearchResult> executeFQNEntityTypeSearch(
            String namespace,
            String name,
            int start,
            int maxHits ) {
        return (Function<ConductorElasticsearchApi, SearchResult> & Serializable) ( api ) -> api
                .executeFQNEntityTypeSearch( namespace, name, start, maxHits );
    }

    public static Function<ConductorElasticsearchApi, SearchResult> executeFQNPropertyTypeSearch(
            String namespace,
            String name,
            int start,
            int maxHits ) {
        return (Function<ConductorElasticsearchApi, SearchResult> & Serializable) ( api ) -> api
                .executeFQNPropertyTypeSearch( namespace, name, start, maxHits );
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

    public static Function<ConductorElasticsearchApi, Boolean> triggerPropertyTypeIndex( List<PropertyType> propertyTypes ) {
        return (Function<ConductorElasticsearchApi, Boolean> & Serializable) ( api ) -> api
                .triggerPropertyTypeIndex( propertyTypes );
    }

    public static Function<ConductorElasticsearchApi, Boolean> triggerEntityTypeIndex( List<EntityType> entityTypes ) {
        return (Function<ConductorElasticsearchApi, Boolean> & Serializable) ( api ) -> api
                .triggerEntityTypeIndex( entityTypes );
    }

    public static Function<ConductorElasticsearchApi, Boolean> triggerAssociationTypeIndex( List<AssociationType> associationTypes ) {
        return (Function<ConductorElasticsearchApi, Boolean> & Serializable) ( api ) -> api
                .triggerAssociationTypeIndex( associationTypes );
    }

    public static Function<ConductorElasticsearchApi, Boolean> triggerEntitySetIndex(
            Map<EntitySet, Set<UUID>> entitySets,
            Map<UUID, PropertyType> propertyTypes ) {
        return (Function<ConductorElasticsearchApi, Boolean> & Serializable) ( api ) -> api
                .triggerEntitySetIndex( entitySets, propertyTypes );
    }

    public static Function<ConductorElasticsearchApi, Boolean> triggerAppIndex( List<App> apps ) {
        return (Function<ConductorElasticsearchApi, Boolean> & Serializable) ( api ) -> api
                .triggerAppIndex( apps );
    }

    public static Function<ConductorElasticsearchApi, Boolean> triggerAppTypeIndex( List<AppType> appTypes ) {
        return (Function<ConductorElasticsearchApi, Boolean> & Serializable) ( api ) -> api
                .triggerAppTypeIndex( appTypes );
    }

    public static Function<ConductorElasticsearchApi, Boolean> triggerOrganizationIndex( List<Organization> organizations ) {
        return (Function<ConductorElasticsearchApi, Boolean> & Serializable) ( api ) -> api
                .triggerOrganizationIndex( organizations );
    }

    public static <T> Function<ConductorElasticsearchApi, Set<UUID>> getEntitySetsWithIndices() {
        return (Function<ConductorElasticsearchApi, Set<UUID>> & Serializable) ConductorElasticsearchApi::getEntityTypesWithIndices;
    }
}
