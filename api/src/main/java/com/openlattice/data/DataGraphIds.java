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
 *
 */

package com.openlattice.data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ListMultimap;
import com.openlattice.client.serialization.SerializationConstants;

import java.util.Objects;
import java.util.UUID;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class DataGraphIds {
    private final ListMultimap<UUID, UUID> entityKeyIds;
    private final ListMultimap<UUID, UUID> associationEntityKeyIds;

    @JsonCreator
    public DataGraphIds(
            @JsonProperty( SerializationConstants.ENTITY_KEY_IDS ) ListMultimap<UUID, UUID> entityKeyIds,
            @JsonProperty( SerializationConstants.ENTITY_SET_IDS ) ListMultimap<UUID, UUID> associationEntityKeyIds ) {
        this.entityKeyIds = entityKeyIds;
        this.associationEntityKeyIds = associationEntityKeyIds;
    }

    @Override public boolean equals( Object o ) {
        if ( this == o ) { return true; }
        if ( !( o instanceof DataGraphIds ) ) { return false; }
        DataGraphIds that = (DataGraphIds) o;
        return Objects.equals( entityKeyIds, that.entityKeyIds ) &&
                Objects.equals( associationEntityKeyIds, that.associationEntityKeyIds );
    }

    @Override public int hashCode() {

        return Objects.hash( entityKeyIds, associationEntityKeyIds );
    }

    @Override public String toString() {
        return "DataGraphIds{" +
                "entityKeyIds=" + entityKeyIds +
                ", associationEntityKeyIds=" + associationEntityKeyIds +
                '}';
    }

    @JsonProperty( SerializationConstants.ENTITY_KEY_IDS )
    public ListMultimap<UUID, UUID> getEntityKeyIds() {
        return entityKeyIds;
    }

    @JsonProperty( SerializationConstants.ENTITY_SET_IDS )
    public ListMultimap<UUID, UUID> getAssociationEntityKeyIds() {
        return associationEntityKeyIds;
    }
}
