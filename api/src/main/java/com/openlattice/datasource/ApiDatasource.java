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

package com.openlattice.datasource;

import com.openlattice.authorization.securable.AbstractSecurableObject;
import com.openlattice.authorization.securable.SecurableObjectType;
import com.openlattice.client.serialization.SerializationConstants;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;

import java.util.Set;
import java.util.UUID;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class ApiDatasource extends AbstractSecurableObject {
    private final Set<UUID> entitySetIds;

    public ApiDatasource(
            UUID id,
            String title,
            Optional<String> description,
            @JsonProperty( SerializationConstants.ENTITY_SET_IDS ) Set<UUID> entitySetIds ) {
        this( Optional.of( id ), title, description, entitySetIds );
    }

    @JsonCreator
    public ApiDatasource(
            @JsonProperty( SerializationConstants.ID_FIELD ) Optional<UUID> id,
            @JsonProperty( SerializationConstants.TITLE_FIELD ) String title,
            @JsonProperty( SerializationConstants.DESCRIPTION_FIELD ) Optional<String> description,
            @JsonProperty( SerializationConstants.ENTITY_SET_IDS ) Set<UUID> entitySetIds ) {
        super( id, title, description );
        this.entitySetIds = entitySetIds;
    }

    @JsonProperty( SerializationConstants.ENTITY_SET_IDS )
    public Set<UUID> getEntitySetIds() {
        return entitySetIds;
    }

    @Override
    @JsonIgnore
    public SecurableObjectType getCategory() {
        return SecurableObjectType.Datasource;
    }
}
