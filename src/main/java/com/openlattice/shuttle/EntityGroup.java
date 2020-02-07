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

public class EntityGroup implements Serializable {

    private static final long serialVersionUID = 3461224551648832764L;

    private final Map<String, EntityDefinition> entityDefinitions;

    @JsonCreator
    public EntityGroup(
            @JsonProperty( SerializationConstants.ENTITY_DEFINITIONS_FIELD )
                    Map<String, EntityDefinition> entityDefinitions ) {
        this.entityDefinitions = entityDefinitions;
    }

    private EntityGroup( EntityGroup.Builder builder ) {
        this.entityDefinitions = builder.entityDefinitionMap;
    }

    @JsonIgnore
    public Collection<EntityDefinition> getEntities() {
        return this.entityDefinitions.values();
    }

    @JsonProperty( SerializationConstants.ENTITY_DEFINITIONS_FIELD )
    public Map<String, EntityDefinition> getEntityDefinitions() {
        return entityDefinitions;
    }

    @Override
    public String toString() {
        return "EntityGroup [entityDefinitions=" + entityDefinitions + "]";
    }

    @Override
    public int hashCode() {

        final int prime = 31;
        int result = 1;
        result = prime * result + ( ( entityDefinitions == null ) ? 0 : entityDefinitions.hashCode() );
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
        EntityGroup other = (EntityGroup) obj;
        if ( entityDefinitions == null ) {
            if ( other.entityDefinitions != null )
                return false;
        } else if ( !entityDefinitions.equals( other.entityDefinitions ) )
            return false;
        return true;
    }

    public static class Builder extends BaseBuilder<Flight.Builder, EntityGroup> {

        private Map<String, EntityDefinition> entityDefinitionMap;

        public Builder(
                Flight.Builder builder,
                BuilderCallback<EntityGroup> builderCallback ) {

            super( builder, builderCallback );
            this.entityDefinitionMap = Maps.newHashMap();
        }

        public EntityDefinition.Builder addEntity( String entityAlias ) {

            BuilderCallback<EntityDefinition> onBuild = entityDefinition -> {
                String alias = entityDefinition.getAlias();
                if ( entityDefinitionMap.containsKey( alias ) ) {
                    throw new IllegalStateException(
                            String.format( "encountered duplicate entity alias: %s", alias ) );
                }
                entityDefinitionMap.put( entityAlias, entityDefinition );
            };

            return new EntityDefinition.Builder( entityAlias, this, onBuild );
        }

        public Flight.Builder ok() {
            return endEntities();
        }

        public Flight.Builder endEntities() {

            if ( this.entityDefinitionMap.size() == 0 ) {
                throw new IllegalStateException( "invoking addEntity() at least once is required" );
            }

            return super.ok( new EntityGroup( this ) );
        }
    }

}
