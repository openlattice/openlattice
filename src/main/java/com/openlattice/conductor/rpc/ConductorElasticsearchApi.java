

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
import com.openlattice.data.EntityDataKey;
import com.openlattice.edm.EntitySet;
import com.openlattice.edm.type.AssociationType;
import com.openlattice.edm.type.EntityType;
import com.openlattice.edm.type.PropertyType;
import com.openlattice.organization.Organization;
import com.openlattice.rhizome.hazelcast.DelegatedUUIDSet;
import com.openlattice.search.requests.*;
import com.google.common.collect.SetMultimap;
import com.openlattice.authorization.AclKey;
import com.openlattice.rhizome.hazelcast.DelegatedStringSet;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

public interface ConductorElasticsearchApi {

    // settings consts
    final String NUM_SHARDS         = "number_of_shards";
    final String NUM_REPLICAS       = "number_of_replicas";
    final String ANALYSIS           = "analysis";
    final String FILTER             = "filter";
    final String ANALYZER           = "analyzer";
    final String ENCODER            = "encoder";
    final String REPLACE            = "replace";
    final String OUTPUT_UNIGRAMS    = "output_unigrams";
    final String TOKEN_SEPARATOR    = "token_separator";
    final String TOKENIZER          = "tokenizer";
    final String STANDARD           = "standard";
    final String LOWERCASE          = "lowercase";
    final String PHONETIC           = "phonetic";
    final String METAPHONE          = "metaphone";
    final String SHINGLE            = "shingle";
    final String METAPHONE_FILTER   = "metaphone_filter";
    final String SHINGLE_FILTER     = "shingle_filter";
    final String METAPHONE_ANALYZER = "MetaphoneAnalyzer";

    final String ES_PROPERTIES = "properties";
    final String PARENT        = "_parent";
    final String TYPE          = "type";
    final String OBJECT        = "object";
    final String NESTED        = "nested";
    final String INDEX         = "index";
    final String NOT_ANALYZED  = "not_analyzed";

    // datatypes
    final String TEXT      = "text";
    final String KEYWORD   = "keyword";
    final String INTEGER   = "integer";
    final String SHORT     = "short";
    final String LONG      = "long";
    final String DOUBLE    = "double";
    final String FLOAT     = "float";
    final String BYTE      = "byte";
    final String DATE      = "date";
    final String BOOLEAN   = "boolean";
    final String BINARY    = "binary";
    final String GEO_POINT = "geo_point";

    // entity_set_data_model setup consts
    final String ENTITY_SET_DATA_MODEL = "entity_set_data_model";
    final String ENTITY_SET_TYPE       = "entity_set";

    // organizations setup consts
    final String ORGANIZATIONS     = "organizations";
    final String ORGANIZATION      = "organization";
    final String ORGANIZATION_TYPE = "organizationType";
    final String ORGANIZATION_ID   = "organizationId";

    final String SECURABLE_OBJECT_INDEX_PREFIX = "securable_object_";
    final String SECURABLE_OBJECT_TYPE_PREFIX  = "type_";
    final String ACL_KEY                       = "aclKey";
    final String PROPERTY_TYPE_ID              = "propertyTypeId";

    // entity_type_index setup consts
    final String ENTITY_TYPE_INDEX = "entity_type_index";
    final String ENTITY_TYPE       = "entity_type";

    // property_type_index setup consts
    final String PROPERTY_TYPE_INDEX = "property_type_index";
    final String PROPERTY_TYPE       = "property_type";

    // association_type_index setup consts
    final String ASSOCIATION_TYPE_INDEX = "association_type_index";
    final String ASSOCIATION_TYPE       = "association_type";
    final String ENTITY_TYPE_FIELD      = "entityType";

    // app_index setup consts
    final String APP_INDEX = "app_index";
    final String APP       = "app";

    // app_type_index setup consts
    final String APP_TYPE_INDEX = "app_type_index";
    final String APP_TYPE       = "app_type";

    // entity set field consts
    final String TYPE_FIELD     = "_type";
    final String ENTITY_SET     = "entitySet";
    final String ENTITY_SET_ID  = "entitySetId";
    final String PROPERTY_TYPES = "propertyTypes";
    final String ACLS           = "acls";
    final String NAME           = "name";
    final String NAMESPACE      = "namespace";
    final String TITLE          = "title";
    final String DESCRIPTION    = "description";
    final String ENTITY_TYPE_ID = "entityTypeId";
    final String ID             = "id";
    final String SRC            = "src";
    final String DST            = "dst";
    final String BIDIRECTIONAL  = "bidirectional";
    final String URL            = "url";

    UUID LAST_WRITE = new UUID( 0, 0 );

    boolean saveEntitySetToElasticsearch( EntitySet entitySet, List<PropertyType> propertyTypes );

    Set<UUID> getEntitySetWithIndices();

    boolean createSecurableObjectIndex( UUID entitySetId, List<PropertyType> propertyTypes );

    boolean deleteEntitySet( UUID entitySetId );

    boolean clearEntitySetData( UUID entitySetId );

    SearchResult executeEntitySetMetadataSearch(
            Optional<String> optionalSearchTerm,
            Optional<UUID> optionalEntityType,
            Optional<Set<UUID>> optionalPropertyTypes,
            Set<AclKey> authorizedAclKeys,
            int start,
            int maxHits );

    boolean updateEntitySetMetadata( EntitySet entitySet );

    boolean updatePropertyTypesInEntitySet( UUID entitySetId, List<PropertyType> newPropertyTypes );

    boolean createOrganization( Organization organization );

    boolean deleteOrganization( UUID organizationId );

    SearchResult executeOrganizationSearch(
            String searchTerm,
            Set<AclKey> authorizedOrganizationIds,
            int start,
            int maxHits );

    boolean updateOrganization( UUID id, Optional<String> optionalTitle, Optional<String> optionalDescription );

    boolean createEntityData(
            EntityDataKey edk,
            Map<UUID, Set<Object>> propertyValues );

    boolean createBulkEntityData( UUID entitySetId, Map<UUID, Map<UUID, Set<Object>>> entitiesById );

    boolean deleteEntityData( EntityDataKey edk );

    EntityDataKeySearchResult executeSearch(
            SearchConstraints searchConstraints,
            Map<UUID, DelegatedUUIDSet> authorizedPropertyTypesByEntitySet
    );

    /**
     * Performs a capped size search across several entity sets.
     *
     * @param entitySetIds  The entity sets to search.
     * @param fieldSearches The values for each field that is being searched.
     * @param size          The size cap on the results per entity set.
     * @param explain
     * @return A map entity set ids to entity key ids of results for each entity set.
     */
    Map<UUID, Set<UUID>> searchEntitySets(
            Iterable<UUID> entitySetIds,
            Map<UUID, DelegatedStringSet> fieldSearches,
            int size,
            boolean explain );

    List<UUID> executeEntitySetDataSearchAcrossIndices(
            Iterable<UUID> entitySetIds,
            Map<UUID, DelegatedStringSet> fieldSearches,
            int size,
            boolean explain );

    boolean saveEntityTypeToElasticsearch( EntityType entityType );

    boolean saveAssociationTypeToElasticsearch( AssociationType associationType );

    boolean savePropertyTypeToElasticsearch( PropertyType propertyType );

    boolean deleteEntityType( UUID entityTypeId );

    boolean deleteAssociationType( UUID associationTypeId );

    boolean deletePropertyType( UUID propertyTypeId );

    SearchResult executeEntityTypeSearch( String searchTerm, int start, int maxHits );

    SearchResult executeAssociationTypeSearch( String searchTerm, int start, int maxHits );

    SearchResult executePropertyTypeSearch( String searchTerm, int start, int maxHits );

    SearchResult executeFQNEntityTypeSearch( String namespace, String name, int start, int maxHits );

    SearchResult executeFQNPropertyTypeSearch( String namespace, String name, int start, int maxHits );

    boolean clearAllData();

    double getModelScore( double[][] features );

    boolean triggerPropertyTypeIndex( List<PropertyType> propertyTypes );

    boolean triggerEntityTypeIndex( List<EntityType> entityTypes );

    boolean triggerAssociationTypeIndex( List<AssociationType> associationTypes );

    boolean triggerEntitySetIndex( Map<EntitySet, Set<UUID>> entitySets, Map<UUID, PropertyType> propertyTypes );

    boolean triggerAppIndex( List<App> apps );

    boolean triggerAppTypeIndex( List<AppType> appTypes );

    boolean triggerOrganizationIndex( List<Organization> organizations );

    boolean saveAppToElasticsearch( App app );

    boolean deleteApp( UUID appId );

    SearchResult executeAppSearch( String searchTerm, int start, int maxHits );

    boolean saveAppTypeToElasticsearch( AppType appType );

    boolean deleteAppType( UUID appTypeId );

    SearchResult executeAppTypeSearch( String searchTerm, int start, int maxHits );

}
