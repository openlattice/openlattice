

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

import com.openlattice.authorization.AclKey;
import com.openlattice.authorization.securable.SecurableObjectType;
import com.openlattice.data.EntityDataKey;
import com.openlattice.edm.EntitySet;
import com.openlattice.edm.type.AssociationType;
import com.openlattice.edm.type.EntityType;
import com.openlattice.edm.type.PropertyType;
import com.openlattice.organization.Organization;
import com.openlattice.rhizome.hazelcast.DelegatedStringSet;
import com.openlattice.rhizome.hazelcast.DelegatedUUIDSet;
import com.openlattice.search.requests.EntityDataKeySearchResult;
import com.openlattice.search.requests.SearchConstraints;
import com.openlattice.search.requests.SearchResult;

import java.util.*;

public interface ConductorElasticsearchApi {

    // settings consts
    String NUM_SHARDS         = "number_of_shards";
    String NUM_REPLICAS       = "number_of_replicas";
    String ANALYSIS           = "analysis";
    String FILTER             = "filter";
    String ANALYZER           = "analyzer";
    String ENCODER            = "encoder";
    String REPLACE            = "replace";
    String OUTPUT_UNIGRAMS    = "output_unigrams";
    String TOKEN_SEPARATOR    = "token_separator";
    String TOKENIZER          = "tokenizer";
    String STANDARD           = "standard";
    String LOWERCASE          = "lowercase";
    String PHONETIC           = "phonetic";
    String METAPHONE          = "metaphone";
    String SHINGLE            = "shingle";
    String METAPHONE_FILTER   = "metaphone_filter";
    String SHINGLE_FILTER     = "shingle_filter";
    String METAPHONE_ANALYZER = "MetaphoneAnalyzer";

    String MAPPING_PROPERTIES = "properties";
    String TYPE               = "type";
    String OBJECT             = "object";
    String NESTED             = "nested";
    String INDEX              = "index";

    // datatypes
    String TEXT      = "text";
    String KEYWORD   = "keyword";
    String INTEGER   = "integer";
    String SHORT     = "short";
    String LONG      = "long";
    String DOUBLE    = "double";
    String FLOAT     = "float";
    String BYTE      = "byte";
    String DATE      = "date";
    String BOOLEAN   = "boolean";
    String GEO_POINT = "geo_point";

    // entity_set_data_model setup consts
    String ENTITY_SET_DATA_MODEL = "entity_set_data_model";
    String ENTITY_SET_TYPE       = "entity_set";

    // organizations setup consts
    String ORGANIZATIONS     = "organizations";
    String ORGANIZATION      = "com/openlattice/organization";
    String ORGANIZATION_TYPE = "organizationType";

    String DATA_INDEX_PREFIX = "entity_data_";
    String DATA_TYPE_PREFIX  = "data_type_";

    // entity_type_index setup consts
    String ENTITY_TYPE_INDEX = "entity_type_index";
    String ENTITY_TYPE       = "entity_type";

    // property_type_index setup consts
    String PROPERTY_TYPE_INDEX = "property_type_index";
    String PROPERTY_TYPE       = "property_type";

    // association_type_index setup consts
    String ASSOCIATION_TYPE_INDEX = "association_type_index";
    String ASSOCIATION_TYPE       = "association_type";

    // app_index setup consts
    String APP_INDEX = "app_index";
    String APP       = "app";

    // app_type_index setup consts
    String APP_TYPE_INDEX = "app_type_index";
    String APP_TYPE       = "app_type";

    // entity set field consts
    String ENTITY_SET     = "entitySet";
    String PROPERTY_TYPES = "propertyTypes";

    // entity type data nested fields
    String ENTITY              = "entity";
    String ENTITY_SET_ID_FIELD = "entitySetId";

    // entity_type_collection_index setup consts
    String ENTITY_TYPE_COLLECTION_INDEX = "entity_type_collection_index";
    String ENTITY_TYPE_COLLECTION       = "entity_type_collection";

    // entity_set_collection_index setup consts
    String ENTITY_SET_COLLECTION_INDEX = "entity_set_collection_index";
    String ENTITY_SET_COLLECTION       = "entity_set_collection";

    UUID LAST_WRITE        = new UUID( 0, 0 );
    UUID ENTITY_SET_ID_KEY = new UUID( 0, 1 );

    Set<UUID> getEntityTypesWithIndices();

    /**
     * Entity Data Create/Update/Delete
     **/

    /* Create */
    boolean createEntityData(
            UUID entityTypeId,
            EntityDataKey edk,
            Map<UUID, Set<Object>> propertyValues );

    boolean createBulkEntityData( UUID entityTypeId, UUID entitySetId, Map<UUID, Map<UUID, Set<Object>>> entitiesById );

    boolean createBulkLinkedData(
            UUID entityTypeId,
            UUID entitySetId,
            Map<UUID, Map<UUID, Map<UUID, Set<Object>>>> entitiesByLinkingId );

    /* Delete */
    boolean deleteEntitySet( UUID entitySetId, UUID entityTypeId );

    boolean clearEntitySetData( UUID entitySetId, UUID entityTypeId );

    boolean deleteEntityData( EntityDataKey edk, UUID entityTypeId );

    boolean deleteEntityDataBulk( UUID entitySetId, UUID entityTypeId, Set<UUID> entityKeyIds );

    /* Update Mappings */
    boolean addPropertyTypesToEntityType( EntityType entityType, List<PropertyType> newPropertyTypes );

    /**
     * Entity Data Search
     **/

    EntityDataKeySearchResult executeSearch(
            SearchConstraints searchConstraints,
            Map<UUID, UUID> entityTypesByEntitySetId,
            Map<UUID, DelegatedUUIDSet> authorizedPropertyTypesByEntitySet,
            Map<UUID, DelegatedUUIDSet> linkingEntitySets
    );

    /**
     * Performs a capped size search across all non-linked entity sets of a particular index.
     * NOTE: permissions are not enforced on this search, so it should not be exposed via the api.
     *
     * @param entityTypeId  The entity type id of the index to search
     * @param fieldSearches The values for each field that is being searched.
     * @param size          The size cap on the results per entity set.
     * @param explain
     * @return A map entity set ids to entity key ids of results for each entity set.
     */
    Map<UUID, Set<UUID>> executeBlockingSearch(
            UUID entityTypeId,
            Map<UUID, DelegatedStringSet> fieldSearches,
            int size,
            boolean explain );

    /**
     * EDM / SecurableObject Create/Update/Delete
     **/


    /* Entity Sets */
    boolean saveEntitySetToElasticsearch(
            EntityType entityType,
            EntitySet entitySet,
            List<PropertyType> propertyTypes );

    boolean updateEntitySetMetadata( EntitySet entitySet );

    boolean updatePropertyTypesInEntitySet( UUID entitySetId, List<PropertyType> updatedPropertyTypes );

    /* Organizations */
    boolean createOrganization( Organization organization );

    boolean updateOrganization( UUID id, Optional<String> optionalTitle, Optional<String> optionalDescription );

    /* Entity/Association Type Creation */
    boolean saveEntityTypeToElasticsearch( EntityType entityType, List<PropertyType> propertyTypes );

    boolean saveAssociationTypeToElasticsearch( AssociationType associationType, List<PropertyType> propertyTypes );

    /* Default SecurableObject create and delete */
    boolean saveSecurableObjectToElasticsearch( SecurableObjectType securableObjectType, Object securableObject );

    boolean deleteSecurableObjectFromElasticsearch( SecurableObjectType securableObjectType, UUID objectId );

    /**
     * EDM / SecurableObject Metadata Searches
     **/

    SearchResult executeSecurableObjectSearch(
            SecurableObjectType securableObjectType,
            String searchTerm,
            int start,
            int maxHits );

    SearchResult executeSecurableObjectFQNSearch(
            SecurableObjectType securableObjectType,
            String namespace,
            String name,
            int start,
            int maxHits );

    SearchResult executeEntitySetCollectionSearch(
            String searchTerm,
            Set<AclKey> authorizedEntitySetCollectionIds,
            int start,
            int maxHits );

    SearchResult executeEntitySetMetadataSearch(
            Optional<String> optionalSearchTerm,
            Optional<UUID> optionalEntityType,
            Optional<Set<UUID>> optionalPropertyTypes,
            Set<AclKey> authorizedAclKeys,
            int start,
            int maxHits );

    SearchResult executeOrganizationSearch(
            String searchTerm,
            Set<AclKey> authorizedOrganizationIds,
            int start,
            int maxHits );

    /**
     * Re-indexing
     **/

    boolean clearAllData();

    boolean triggerEntitySetIndex( Map<EntitySet, Set<UUID>> entitySets, Map<UUID, PropertyType> propertyTypes );

    boolean triggerOrganizationIndex( List<Organization> organizations );

    boolean triggerSecurableObjectIndex(
            SecurableObjectType securableObjectType,
            Iterable<?> securableObjects );

}
