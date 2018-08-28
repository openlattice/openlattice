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

package com.openlattice.graph.query;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.client.serialization.SerializationConstants;
import java.time.OffsetDateTime;
import java.util.Objects;
import java.util.UUID;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class ResultSummary {
    private final UUID           queryId;
    private final long           entityCount;
    private final long           edgeCount;
    private final OffsetDateTime expiration;

    @JsonCreator
    public ResultSummary(
            @JsonProperty( SerializationConstants.QUERY_ID ) UUID queryId,
            @JsonProperty( SerializationConstants.ENTITY_COUNT ) long entityCount,
            @JsonProperty( SerializationConstants.EDGE_COUNT ) long edgeCount,
            @JsonProperty( SerializationConstants.EXPIRATION ) OffsetDateTime expiration ) {
        this.queryId = queryId;
        this.entityCount = entityCount;
        this.edgeCount = edgeCount;
        this.expiration = expiration;
    }

    @JsonProperty( SerializationConstants.QUERY_ID )
    public UUID getQueryId() {
        return queryId;
    }

    @JsonProperty( SerializationConstants.ENTITY_COUNT )
    public long getEntityCount() {
        return entityCount;
    }

    @JsonProperty( SerializationConstants.EDGE_COUNT )
    public long getEdgeCount() {
        return edgeCount;
    }

    @JsonProperty( SerializationConstants.EXPIRATION )
    public OffsetDateTime getExpiration() {
        return expiration;
    }

    @Override public boolean equals( Object o ) {
        if ( this == o ) { return true; }
        if ( !( o instanceof ResultSummary ) ) { return false; }
        ResultSummary that = (ResultSummary) o;
        return entityCount == that.entityCount &&
                edgeCount == that.edgeCount &&
                Objects.equals( queryId, that.queryId ) &&
                Objects.equals( expiration, that.expiration );
    }

    @Override public int hashCode() {

        return Objects.hash( queryId, entityCount, edgeCount, expiration );
    }
}
