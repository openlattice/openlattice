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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.client.serialization.SerializationConstants;
import com.openlattice.edm.EntitySet;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

public class NeighborEntityDetails {
    private final EntitySet                              associationEntitySet;
    private final Map<FullQualifiedName, Set<Object>> associationDetails;

    private final Optional<EntitySet>                           neighborEntitySet;
    private final Optional<UUID>                                neighborId;
    private final Optional<Map<FullQualifiedName, Set<Object>>> neighborDetails;

    private final boolean entityIsSrc;

    @JsonCreator
    public NeighborEntityDetails(
            @JsonProperty( SerializationConstants.ASSOCIATION_ENTITY_SET ) EntitySet associationEntitySet,
            @JsonProperty( SerializationConstants.ASSOCIATION_DETAILS )
                    Map<FullQualifiedName, Set<Object>> associationDetails,
            @JsonProperty( SerializationConstants.NEIGHBOR_ENTITY_SET ) Optional<EntitySet> neighborEntitySet,
            @JsonProperty( SerializationConstants.NEIGHBOR_ID ) Optional<UUID> neighborId,
            @JsonProperty( SerializationConstants.NEIGHBOR_DETAILS )
                    Optional<Map<FullQualifiedName, Set<Object>>> neighborDetails,
            @JsonProperty( SerializationConstants.SRC ) boolean entityIsSrc ) {
        this.associationEntitySet = associationEntitySet;
        this.associationDetails = associationDetails;
        this.neighborEntitySet = neighborEntitySet;
        this.neighborId = neighborId;
        this.neighborDetails = neighborDetails;
        this.entityIsSrc = entityIsSrc;
    }

    public NeighborEntityDetails(
            EntitySet associationEntitySet,
            Map<FullQualifiedName, Set<Object>> associationDetails,
            EntitySet neighborEntitySet,
            UUID neighborId,
            Map<FullQualifiedName, Set<Object>> neighborDetails,
            boolean entityIsSrc ) {
        this(
                associationEntitySet,
                associationDetails,
                Optional.of( neighborEntitySet ),
                Optional.of( neighborId ),
                Optional.of( neighborDetails ),
                entityIsSrc );
    }

    public NeighborEntityDetails(
            EntitySet associationEntitySet,
            Map<FullQualifiedName, Set<Object>> associationDetails,
            boolean entityIsSrc ) {
        this(
                associationEntitySet,
                associationDetails,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                entityIsSrc );
    }

    @JsonProperty( SerializationConstants.ASSOCIATION_ENTITY_SET )
    public EntitySet getAssociationEntitySet() {
        return associationEntitySet;
    }

    @JsonProperty( SerializationConstants.ASSOCIATION_DETAILS )
    public Map<FullQualifiedName, Set<Object>> getAssociationDetails() {
        return associationDetails;
    }

    @JsonProperty( SerializationConstants.NEIGHBOR_ENTITY_SET )
    public Optional<EntitySet> getNeighborEntitySet() {
        return neighborEntitySet;
    }

    @JsonProperty( SerializationConstants.NEIGHBOR_ID )
    public Optional<UUID> getNeighborId() {
        return neighborId;
    }

    @JsonProperty( SerializationConstants.NEIGHBOR_DETAILS )
    public Optional<Map<FullQualifiedName, Set<Object>>> getNeighborDetails() {
        return neighborDetails;
    }

    @JsonProperty( SerializationConstants.SRC )
    public boolean getEntityIsSrc() {
        return entityIsSrc;
    }

}
