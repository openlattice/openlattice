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

package com.openlattice.data.requests;


import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.openlattice.client.serialization.SerializationConstants;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class LookupEntitiesRequest {
    private final Set<FullQualifiedName>         entityTypes;
    private final Map<FullQualifiedName, Object> propertyTypeToValueMap;
    private final UUID                           userId;

    public LookupEntitiesRequest(
            UUID userId,
            Set<FullQualifiedName> entityTypes,
            Map<FullQualifiedName, Object> propertyTypeToValueMap ) {
        this.entityTypes = entityTypes;
        this.propertyTypeToValueMap = propertyTypeToValueMap;
        this.userId = userId;
    }

    public Set<FullQualifiedName> getEntityTypes() {
        return entityTypes;
    }

    public Map<FullQualifiedName, Object> getPropertyTypeToValueMap() {
        return propertyTypeToValueMap;
    }

    public UUID getUserId() {
        return userId;
    }
    
    @JsonCreator
    public static LookupEntitiesRequest newLookupEntitiesRequest(
    		@JsonProperty( SerializationConstants.USER_ID ) UUID userId,
    		@JsonProperty( SerializationConstants.TYPE_FIELD ) Set<FullQualifiedName> entityTypes,
    		@JsonProperty( SerializationConstants.PROPERTIES_FIELD ) Map<String, Object> propertyMapInString) {
    	//Create propertyValues with FullQualifiedName as key
    	Map<FullQualifiedName, Object> propertyMapInFQN = propertyMapInString.entrySet()
    			.stream()
    			.collect(Collectors.toMap(
    						entry -> new FullQualifiedName( entry.getKey() ),
    						entry -> entry.getValue()
    					)
    			);
    	
        return new LookupEntitiesRequest(
        		userId,
        		entityTypes,
        		propertyMapInFQN
        		);
    } 

}
