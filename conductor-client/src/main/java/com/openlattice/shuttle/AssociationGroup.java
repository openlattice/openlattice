/*
 * Copyright (C) 2020. OpenLattice, Inc
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
 */
package com.openlattice.shuttle;

import com.openlattice.client.serialization.SerializationConstants;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Maps;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class AssociationGroup implements Serializable {

    private static final long serialVersionUID = 2916195784263120298L;

    private final Map<String, AssociationDefinition> associationDefinitions;

    @JsonCreator
    public AssociationGroup(
            @JsonProperty( SerializationConstants.ASSOCIATION_DEFINITIONS_FIELD )
                    Map<String, AssociationDefinition> associationDefinitions ) {
        this.associationDefinitions = associationDefinitions;
    }

    private AssociationGroup( AssociationGroup.Builder builder ) {
        this.associationDefinitions = builder.associationDefinitionMap;
    }

    @JsonIgnore
    public Collection<AssociationDefinition> getAssociations() {
        return this.associationDefinitions.values();
    }

    @JsonProperty( SerializationConstants.ASSOCIATION_DEFINITIONS_FIELD )
    public Map<String, AssociationDefinition> getAssociationDefinitions() {
        return associationDefinitions;
    }

    @Override
    public String toString() {
        return "AssociationGroup [associationDefinitions=" + associationDefinitions + "]";
    }

    @Override
    public int hashCode() {

        final int prime = 31;
        int result = 1;
        result = prime * result + ( ( associationDefinitions == null ) ? 0 : associationDefinitions.hashCode() );
        return result;
    }

    @Override
    public boolean equals( Object obj ) {

        if ( this == obj )
            return true;
        if ( obj == null )
            return false;
        if ( getClass() != obj.getClass() )
            return false;
        AssociationGroup other = (AssociationGroup) obj;
        if ( associationDefinitions == null ) {
            if ( other.associationDefinitions != null )
                return false;
        } else if ( !associationDefinitions.equals( other.associationDefinitions ) )
            return false;
        return true;
    }

    public static class Builder extends BaseBuilder<Flight.Builder, AssociationGroup> {

        private Map<String, AssociationDefinition> associationDefinitionMap;
        private Set<String>                        entityAliases;

        public Builder(
                Set<String> entityAliases,
                Flight.Builder builder,
                BuilderCallback<AssociationGroup> builderCallback ) {

            super( builder, builderCallback );

            this.associationDefinitionMap = Maps.newHashMap();
            this.entityAliases = entityAliases;
        }

        public AssociationDefinition.Builder addAssociation( String associationAlias ) {

            BuilderCallback<AssociationDefinition> onBuild = associationDefinition -> {
                String alias = associationDefinition.getAlias();
                if ( associationDefinitionMap.containsKey( alias ) ) {
                    throw new IllegalStateException(
                            String.format( "encountered duplicate association alias: %s", alias ) );
                }
                associationDefinitionMap.put( associationAlias, associationDefinition );
            };

            return new AssociationDefinition.Builder( associationAlias, entityAliases, this, onBuild );
        }

        public Flight.Builder ok() {
            return endAssociations();
        }

        public Flight.Builder endAssociations() {
            return super.ok( new AssociationGroup( this ) );
        }
    }

}
