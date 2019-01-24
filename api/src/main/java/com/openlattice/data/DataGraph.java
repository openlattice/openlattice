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
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.openlattice.client.serialization.SerializationConstants;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class DataGraph {
    private final ListMultimap<UUID, Map<UUID, Set<Object>>> entities;
    private final ListMultimap<UUID, DataAssociation>        associations;

    @JsonCreator
    public DataGraph(
            @JsonProperty( SerializationConstants.ENTITIES ) ListMultimap<UUID, Map<UUID, Set<Object>>> entities,
            @JsonProperty( SerializationConstants.ASSOCIATIONS ) ListMultimap<UUID, DataAssociation> associations ) {
        this.entities = entities;
        this.associations = associations;
    }

    @JsonProperty( SerializationConstants.ENTITIES )
    public ListMultimap<UUID, Map<UUID, Set<Object>>> getEntities() {
        return entities;
    }

    @JsonProperty( SerializationConstants.ASSOCIATIONS )
    public ListMultimap<UUID, DataAssociation> getAssociations() {
        return associations;
    }

    @Override public String toString() {
        return "DataGraph{" +
                "entities=" + entities +
                ", associations=" + associations +
                '}';
    }

    @Override public boolean equals( Object o ) {
        if ( this == o ) { return true; }
        if ( !( o instanceof DataGraph ) ) { return false; }
        DataGraph dataGraph = (DataGraph) o;
        return Objects.equals( entities, dataGraph.entities ) &&
                Objects.equals( associations, dataGraph.associations );
    }

    @Override public int hashCode() {

        return Objects.hash( entities, associations );
    }

    /**
     * Compatibility method for building data graphs from set multimaps instead of maps of sets.
     * @param entities The entities in the data graph.
     * @param associations The associations in the data graph.
     * @return A data graph object.
     */
    public static DataGraph fromMultimap(
            @JsonProperty( SerializationConstants.ENTITIES ) ListMultimap<UUID, SetMultimap<UUID, Object>> entities,
            @JsonProperty( SerializationConstants.ASSOCIATIONS ) ListMultimap<UUID, DataAssociation> associations ) {
        return new DataGraph( Multimaps.transformValues( entities, Multimaps::asMap ), associations );
    }
}
