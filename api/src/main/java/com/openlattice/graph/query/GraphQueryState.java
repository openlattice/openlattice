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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.client.serialization.SerializationConstants;
import com.openlattice.graph.SubGraph;
import java.util.Optional;
import java.util.UUID;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@JsonInclude( value = Include.NON_ABSENT )
public class GraphQueryState {
    private final UUID                    queryId;
    private final State                   state;
    private final long                    elapsedMillis;
    private       Optional<ResultSummary> resultSummary;
    private       Optional<SubGraph>      result;

    @JsonCreator
    public GraphQueryState(
            @JsonProperty( SerializationConstants.QUERY_ID ) UUID queryId,
            @JsonProperty( SerializationConstants.STATE ) State state,
            @JsonProperty( SerializationConstants.RESULT_SUMMARY ) Optional<ResultSummary> resultSummary,
            @JsonProperty( SerializationConstants.ELAPSED_MILLIS ) long elapsedMillis,
            @JsonProperty( SerializationConstants.RESULT ) Optional<SubGraph> result ) {
        this.queryId = queryId;
        this.state = state;
        this.resultSummary = resultSummary;
        this.elapsedMillis = elapsedMillis;
        this.result = result;
    }

    public GraphQueryState( UUID queryId, State state, long elapsedMillis ) {
        this( queryId, state, Optional.empty(), elapsedMillis, Optional.empty() );
    }

    @JsonProperty( SerializationConstants.QUERY_ID )
    public UUID getQueryId() {
        return queryId;
    }

    @JsonProperty( SerializationConstants.STATE )
    public State getState() {
        return state;
    }

    @JsonProperty( SerializationConstants.RESULT_SUMMARY )
    public Optional<ResultSummary> getResultSummary() {
        return resultSummary;
    }

    public void setResultSummary( Optional<ResultSummary> resultSummary ) {
        this.resultSummary = resultSummary;
    }

    @JsonProperty( SerializationConstants.ELAPSED_MILLIS )
    public long getElapsedMillis() {
        return elapsedMillis;
    }

    @JsonProperty( SerializationConstants.RESULT )
    public Optional<SubGraph> getResult() {
        return result;
    }

    public void setResult( Optional<SubGraph> result ) {
        this.result = result;
    }

    public enum State {
        RUNNING,
        COMPLETE,
        STOPPING,
        STOPPED
    }

    public enum Option {
        RESULT,
        SUMMARY
    }
}
