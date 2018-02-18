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

import java.util.Collection;
import java.util.UUID;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.openlattice.client.serialization.SerializationConstants;
import com.openlattice.edm.EntitySet;
import com.openlattice.edm.type.PropertyType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.collect.SetMultimap;

public class NeighborEntityDetails {
    private EntitySet                                        associationEntitySet;
    private SetMultimap<FullQualifiedName, Object>           associationDetails;
    private Collection<PropertyType>                         associationPropertyTypes;

    private Optional<EntitySet>                              neighborEntitySet;
    private Optional<UUID>                                   neighborId;
    private Optional<SetMultimap<FullQualifiedName, Object>> neighborDetails;
    private Optional<Collection<PropertyType>>               neighborPropertyTypes;

    private boolean                                          entityIsSrc;

    @JsonCreator
    public NeighborEntityDetails(
            @JsonProperty( SerializationConstants.ASSOCIATION_ENTITY_SET ) EntitySet associationEntitySet,
            @JsonProperty( SerializationConstants.ASSOCIATION_DETAILS ) SetMultimap<FullQualifiedName, Object> associationDetails,
            @JsonProperty( SerializationConstants.ASSOCIATION_PROPERTY_TYPES ) Collection<PropertyType> associationPropertyTypes,
            @JsonProperty( SerializationConstants.NEIGHBOR_ENTITY_SET ) Optional<EntitySet> neighborEntitySet,
            @JsonProperty( SerializationConstants.NEIGHBOR_ID ) Optional<UUID> neighborId,
            @JsonProperty( SerializationConstants.NEIGHBOR_DETAILS ) Optional<SetMultimap<FullQualifiedName, Object>> neighborDetails,
            @JsonProperty( SerializationConstants.NEIGHBOR_PROPERTY_TYPES ) Optional<Collection<PropertyType>> neighborPropertyTypes,
            @JsonProperty( SerializationConstants.SRC ) boolean entityIsSrc ) {
        this.associationEntitySet = associationEntitySet;
        this.associationDetails = associationDetails;
        this.associationPropertyTypes = associationPropertyTypes;
        this.neighborEntitySet = neighborEntitySet;
        this.neighborId = neighborId;
        this.neighborDetails = neighborDetails;
        this.neighborPropertyTypes = neighborPropertyTypes;
        this.entityIsSrc = entityIsSrc;
    }

    public NeighborEntityDetails(
            EntitySet associationEntitySet,
            SetMultimap<FullQualifiedName, Object> associationDetails,
            Collection<PropertyType> associationPropertyTypes,
            EntitySet neighborEntitySet,
            UUID neighborId,
            SetMultimap<FullQualifiedName, Object> neighborDetails,
            Collection<PropertyType> neighborPropertyTypes,
            boolean entityIsSrc ) {
        this(
                associationEntitySet,
                associationDetails,
                associationPropertyTypes,
                Optional.of( neighborEntitySet ),
                Optional.of( neighborId ),
                Optional.of( neighborDetails ),
                Optional.of( neighborPropertyTypes ),
                entityIsSrc );
    }

    public NeighborEntityDetails(
            EntitySet associationEntitySet,
            SetMultimap<FullQualifiedName, Object> associationDetails,
            Collection<PropertyType> associationPropertyTypes,
            boolean entityIsSrc ) {
        this(
                associationEntitySet,
                associationDetails,
                associationPropertyTypes,
                Optional.absent(),
                Optional.absent(),
                Optional.absent(),
                Optional.absent(),
                entityIsSrc );
    }

    @JsonProperty( SerializationConstants.ASSOCIATION_ENTITY_SET )
    public EntitySet getAssociationEntitySet() {
        return associationEntitySet;
    }

    @JsonProperty( SerializationConstants.ASSOCIATION_DETAILS )
    public SetMultimap<FullQualifiedName, Object> getAssociationDetails() {
        return associationDetails;
    }

    @JsonProperty( SerializationConstants.ASSOCIATION_PROPERTY_TYPES )
    public Collection<PropertyType> getAssociationPropertyTypes() {
        return associationPropertyTypes;
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
    public Optional<SetMultimap<FullQualifiedName, Object>> getNeighborDetails() {
        return neighborDetails;
    }

    @JsonProperty( SerializationConstants.NEIGHBOR_PROPERTY_TYPES )
    public Optional<Collection<PropertyType>> getNeighborPropertyTypes() {
        return neighborPropertyTypes;
    }

    @JsonProperty( SerializationConstants.SRC )
    public boolean getEntityIsSrc() {
        return entityIsSrc;
    }

}
