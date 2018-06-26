/*
 * Copyright (C) 2018. OpenLattice, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * You can contact the owner of the copyright at support@openlattice.com
 */

package com.openlattice.client.serialization;

public final class SerializationConstants {

    public static final String ACL_ID_FIELD                     = "aclId";
    public static final String ACTION                           = "action";
    public static final String DATATYPE_FIELD                   = "datatype";
    public static final String DEIDENTIFIED                     = "deidentified";
    public static final String DETAILS_FIELD                    = "details";
    public static final String DESCRIPTION_FIELD                = "description";
    public static final String ENTITY_DEFINITIONS_FIELD         = "entityDefinitions";
    public static final String ASSOCIATION_DEFINITIONS_FIELD    = "associationDefinitions";
    public static final String ASSOCIATION_TYPES                = "associationTypes";
    public static final String ACL_KEY                          = "aclKey";
    public static final String AUDIT_ID                         = "auditId";
    public static final String BLOCK_ID                         = "blockId";
    public static final String DATA_ID                          = "dataId";
    public static final String DEFAULT_SHOW                     = "defaultShow";
    public static final String EDM_TEMPLATES_FIELD              = "edmTemplates";
    public static final String ENTITY_FIELD                     = "entity";
    public static final String ENTITY_ID                        = "entityId";
    public static final String ENTITY_SET_FIELD                 = "entitySet";
    public static final String ENTITY_SETS_FIELD                = "entitySets";
    public static final String ENTITY_SET_NAME                  = "entitySetName";
    public static final String ENTITY_SET_ID                    = "entitySetId";
    public static final String ENTITY_SET_IDS                   = "entitySetIds";
    public static final String ENTITY_TYPE                      = "entityType";
    public static final String ENTITY_TYPES                     = "entityTypes";
    public static final String ENTITY_TYPE_IDS_FIELD            = "entityTypeIds";
    public static final String EXTERNAL                         = "external";
    public static final String FQN                              = "fqn";
    public static final String KEY_FIELD                        = "key";
    public static final String ID_FIELD                         = "id";
    public static final String IS_OWNER                         = "isOwner";
    public static final String LINKING_ENTITY_SET_FIELD         = "linkingEntitySet";
    public static final String LINKING_PROPERTIES_FIELD         = "linkingProperties";
    public static final String NAME_FIELD                       = "name";
    public static final String NAMESPACE_FIELD                  = "namespace";
    public static final String PERMISSIONS                      = "permissions";
    public static final String PERMISSIONS_MAP                  = "permissionsMap";
    public static final String PRINCIPAL                        = "principal";
    public static final String PROFILE_FIELD                    = "profile";
    public static final String PROPERTIES_FIELD                 = "properties";
    public static final String PROPERTY_FIELD                   = "property";
    public static final String PROPERTY_DEFINITIONS             = "propertyDefinitions";
    public static final String PROPERTY_TYPES                   = "propertyTypes";
    public static final String PROPERTY_TYPE_ID_LIST            = "propertyTypeIds";
    public static final String PROPERTY_TYPE_ID                 = "propertyTypeId";
    public static final String SYNC_ID                          = "syncId";
    public static final String SYNC_IDS                         = "syncIds";
    public static final String SYNC_TICKETS                     = "syncTickets";
    public static final String REQUEST                          = "request";
    public static final String REQUEST_ID                       = "requestId";
    public static final String REQUESTING_USER                  = "requestingUser";
    public static final String REQUEST_STATUS                   = "status";
    public static final String SCHEMA                           = "schema";
    public static final String SCHEMAS                          = "schemas";
    public static final String TIME_ID                          = "timeId";
    public static final String TIMESTAMP                        = "timestamp";
    public static final String TITLE_FIELD                      = "title";                   // for EntitySet
    public static final String TYPE_FIELD                       = "type";
    public static final String USER_ID                          = "userId";
    public static final String ACL                              = "acl";
    public static final String ACE                              = "ace";
    public static final String ACES                             = "aces";
    public static final String ACL_OBJECT_PATH                  = "aclKey";                  // Correspond to single hierarchy of securable objects.
    public static final String ACL_OBJECT_ROOT                  = "aclRoot";
    public static final String ACL_OBJECT_CHILD                 = "aclChild";
    public static final String ENTITIES                         = "entities";
    public static final String ASSOCIATIONS                     = "associations";
    public static final String ENTITY_ID_GENERATOR              = "entityIdGenerator";
    public static final String MEMBERS_FIELD                    = "members";
    public static final String ROLES                            = "roles";
    public static final String TRUSTED_ORGANIZATIONS_FIELD      = "trustedOrganizations";
    public static final String EMAILS_FIELD                     = "emails";
    public static final String ENTITY_TYPE_ID_FIELD             = "entityTypeId";
    public static final String STATUS                           = "status";
    public static final String APPS                             = "apps";
    public static final String KEYWORD                          = "kw";
    public static final String ENTITY_TYPE_ID                   = "eid";
    public static final String PROPERTY_TYPE_IDS                = "pid";
    public static final String INCLUDE_FIELD                    = "include";
    public static final String SEARCH_TERM                      = "searchTerm";
    public static final String SEARCH_FIELDS                    = "searchFields";
    public static final String EXACT_SEARCH_FIELDS              = "exactSearchFields";
    public static final String MAX_HITS                         = "maxHits";
    public static final String NUM_HITS                         = "numHits";
    public static final String HITS                             = "hits";
    public static final String START                            = "start";
    public static final String EXACT                            = "exact";
    public static final String VALUE_MAPPER                     = "valueMapper";
    public static final String PII_FIELD                        = "piiField";
    public static final String ANALYZER                         = "analyzer";
    public static final String CONTACTS                         = "contacts";
    public static final String PAGING_TOKEN                     = "pagingToken";
    public static final String AUTHORIZED_OBJECTS               = "authorizedObjects";
    public static final String REASON                           = "reason";
    public static final String ORGANIZATION_ID                  = "organizationId";
    public static final String FLAGS_FIELD                      = "flags";
    public static final String PARENT_TYPE_FIELD                = "parentType";
    public static final String BASE_TYPE_FIELD                  = "baseType";
    public static final String ERROR                            = "error";
    public static final String ERRORS                           = "errors";
    public static final String MESSAGE                          = "message";
    public static final String HTTP_STATUS_CODE                 = "code";
    public static final String NAMESPACE                        = "namespace";
    public static final String NAME                             = "name";
    public static final String SRC                              = "src";
    public static final String DST                              = "dst";
    public static final String BIDIRECTIONAL                    = "bidirectional";
    public static final String CATEGORY                         = "category";
    public static final String EXPLANATION                      = "explanation";
    public static final String ASSOCIATION_ENTITY_SET           = "associationEntitySet";
    public static final String ASSOCIATION_ENTITY_TYPE          = "associationEntityType";
    public static final String ASSOCIATION_DETAILS              = "associationDetails";
    public static final String ASSOCIATION_PROPERTY_TYPES       = "associationPropertyTypes";
    public static final String NEIGHBOR_ENTITY_SET              = "neighborEntitySet";
    public static final String NEIGHBOR_ENTITY_TYPE             = "neighborEntityType";
    public static final String NEIGHBOR_ID                      = "neighborId";
    public static final String NEIGHBOR_DETAILS                 = "neighborDetails";
    public static final String NEIGHBOR_PROPERTY_TYPES          = "neighborPropertyTypes";
    public static final String ASSOCIATION_TYPE_ID              = "associationTypeId";
    public static final String NEIGHBOR_TYPE_IDS                = "neighborTypeIds";
    public static final String UTILIZER_IS_SRC                  = "utilizerIsSrc";
    public static final String CURRENT_SYNC                     = "currentSync";
    public static final String CONFLICTS                        = "conflicts";
    public static final String DIFF                             = "diff";
    public static final String APP_TYPE_IDS_FIELD               = "appTypeIds";
    public static final String APP_ID                           = "appId";
    public static final String APP_TYPE_ID                      = "appTypeId";
    public static final String ORGANIZATION                     = "organization";
    public static final String CONFIG                           = "config";
    public static final String VERSION                          = "version";
    public static final String VERSIONS                         = "versions";
    public static final String URL                              = "url";
    public static final String VALUE_FIELD                      = "value";
    public static final String HASH                             = "hash";
    public static final String ENTITY_COUNT                     = "entityCount";
    public static final String ASSOCIATION_COUNT                = "associationCount";
    public static final String ASSOCIATIONS_ENTITY_KEY_MAPPINGS = "associationsEntityKeyMappings";
    public static final String ENTITY_SETS_ENTITY_KEY_MAPPINGS  = "entitySetsEntityKeyMappings";
    public static final String EDGE                             = "edge";
    public static final String MISSING                          = "missing";
    public static final String PRESENT                          = "present";
    public static final String IDS                              = "ids";

    private SerializationConstants() {
    }

}
