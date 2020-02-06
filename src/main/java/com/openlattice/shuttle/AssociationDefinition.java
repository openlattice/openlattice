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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.openlattice.client.serialization.SerializableFunction;
import com.openlattice.client.serialization.SerializationConstants;
import com.openlattice.data.UpdateType;
import com.openlattice.shuttle.conditions.Condition;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import com.openlattice.shuttle.transformations.Transformation;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AssociationDefinition extends EntityDefinition implements Serializable {

    private static final long serialVersionUID = -6632902802080642647L;

    private static final Logger logger = LoggerFactory
            .getLogger( AssociationDefinition.class );

    private final String srcAlias;
    private final String dstAlias;

    @JsonCreator
    public AssociationDefinition(
            @JsonProperty( SerializationConstants.FQN ) String entityTypeFqn,
            @JsonProperty( SerializationConstants.ENTITY_SET_NAME ) String entitySetName,
            @JsonProperty( SerializationConstants.KEY_FIELD ) List<FullQualifiedName> key,
            @JsonProperty( SerializationConstants.SRC ) String srcAlias,
            @JsonProperty( SerializationConstants.DST ) String dstAlias,
            @JsonProperty( SerializationConstants.PROPERTY_DEFINITIONS )
                    Map<FullQualifiedName, PropertyDefinition> propertyDefinitions,
            @JsonProperty( SerializationConstants.CONDITIONS ) Optional<List<Condition>> condition,
            @JsonProperty( SerializationConstants.NAME ) String alias,
            @JsonProperty( SerializationConstants.GENERATOR ) Optional<SerializableFunction<Map<String, Object>, String>> generator,
            @JsonProperty( SerializationConstants.UPDATE_TYPE ) Optional<UpdateType> updateType ) {
        super( Optional.empty(),
                entityTypeFqn,
                entitySetName,
                key,
                propertyDefinitions,
                alias,
                condition,
                generator,
                updateType );
        this.srcAlias = srcAlias;
        this.dstAlias = dstAlias;
    }

    private AssociationDefinition( AssociationDefinition.Builder builder ) {
        super(
                ( builder.entityTypeFqn == null ) ? null : builder.entityTypeFqn.getFullQualifiedNameAsString(),
                builder.entitySetName,
                builder.key,
                builder.propertyDefinitionMap,
                Optional.ofNullable( builder.generator ),
                builder.alias == null ? builder.entitySetName : builder.alias,
                builder.updateType );
        this.srcAlias = builder.srcAlias;
        this.dstAlias = builder.dstAlias;
    }

    @JsonIgnore
    public FullQualifiedName getEntityTypeFqn() {
        return this.entityTypeFqn;
    }

    @JsonProperty( SerializationConstants.FQN )
    public String getFqn() {
        return this.entityTypeFqn == null ? null : this.entityTypeFqn.getFullQualifiedNameAsString();
    }

    @JsonProperty( SerializationConstants.ENTITY_SET_NAME )
    public String getEntitySetName() {
        return this.entitySetName;
    }

    @JsonProperty( SerializationConstants.NAME )
    public String getAlias() {
        return this.alias;
    }

    @JsonProperty( SerializationConstants.SRC )
    public String getSrcAlias() {
        return this.srcAlias;
    }

    @JsonProperty( SerializationConstants.DST )
    public String getDstAlias() {
        return this.dstAlias;
    }

    @JsonProperty( SerializationConstants.KEY_FIELD )
    public List<FullQualifiedName> getKey() {
        return key;
    }

    @JsonProperty( SerializationConstants.PROPERTY_DEFINITIONS )
    public Map<FullQualifiedName, PropertyDefinition> getPropertyDefinitions() {
        return propertyDefinitions;
    }

    @JsonProperty( SerializationConstants.ENTITY_ID_GENERATOR )
    public Optional<SerializableFunction<Map<String, Object>, String>> getGenerator() {
        return generator;
    }

    @JsonProperty( SerializationConstants.CONDITIONS )
    public Optional<List<Condition>> getCondition() {
        return condition;
    }

    @JsonIgnore
    public Collection<PropertyDefinition> getProperties() {
        return this.propertyDefinitions.values();
    }

    @Override
    public String toString() {
        return "AssociationDefinition [entityTypeFqn=" + entityTypeFqn + ", entitySetName=" + entitySetName
                + ", srcAlias=" + srcAlias + ", dstAlias=" + dstAlias + ", propertyDefinitions=" + propertyDefinitions
                + ", key=" + key + ", alias=" + alias + ", generator=" + generator + "]";
    }

    @Override
    public int hashCode() {

        final int prime = 31;
        int result = 1;
        result = prime * result + ( ( alias == null ) ? 0 : alias.hashCode() );
        result = prime * result + ( ( dstAlias == null ) ? 0 : dstAlias.hashCode() );
        result = prime * result + ( ( entitySetName == null ) ? 0 : entitySetName.hashCode() );
        result = prime * result + ( ( entityTypeFqn == null ) ? 0 : entityTypeFqn.hashCode() );
        result = prime * result + ( ( generator == null ) ? 0 : generator.hashCode() );
        result = prime * result + ( ( key == null ) ? 0 : key.hashCode() );
        result = prime * result + ( ( propertyDefinitions == null ) ? 0 : propertyDefinitions.hashCode() );
        result = prime * result + ( ( srcAlias == null ) ? 0 : srcAlias.hashCode() );
        return result;
    }

    @Override
    public boolean equals( Object obj ) {

        if ( this == obj ) { return true; }
        if ( obj == null ) { return false; }
        if ( getClass() != obj.getClass() ) { return false; }
        AssociationDefinition other = (AssociationDefinition) obj;

        if ( alias == null ) {
            if ( other.alias != null ) { return false; }
        } else if ( !alias.equals( other.alias ) ) { return false; }

        if ( dstAlias == null ) {
            if ( other.dstAlias != null ) { return false; }
        } else if ( !dstAlias.equals( other.dstAlias ) ) { return false; }

        if ( entitySetName == null ) {
            if ( other.entitySetName != null ) { return false; }
        } else if ( !entitySetName.equals( other.entitySetName ) ) { return false; }

        if ( entityTypeFqn == null ) {
            if ( other.entityTypeFqn != null ) { return false; }
        } else if ( !entityTypeFqn.equals( other.entityTypeFqn ) ) { return false; }

        if ( generator == null ) {
            if ( other.generator != null ) { return false; }
        } else if ( !generator.equals( other.generator ) ) { return false; }

        if ( key == null ) {
            if ( other.key != null ) { return false; }
        } else if ( !key.equals( other.key ) ) { return false; }

        if ( propertyDefinitions == null ) {
            if ( other.propertyDefinitions != null ) { return false; }
        } else if ( !propertyDefinitions.equals( other.propertyDefinitions ) ) { return false; }

        if ( srcAlias == null ) {
            if ( other.srcAlias != null ) { return false; }
        } else if ( !srcAlias.equals( other.srcAlias ) ) { return false; }

        return true;
    }

    public static class Builder extends BaseBuilder<AssociationGroup.Builder, AssociationDefinition> {

        private FullQualifiedName                                 entityTypeFqn;
        private String                                            entitySetName;
        private String                                            srcAlias;
        private String                                            dstAlias;
        private Map<FullQualifiedName, PropertyDefinition>        propertyDefinitionMap;
        private SerializableFunction<Map<String, Object>, String> generator;
        private List<FullQualifiedName>                           key;
        private String                                            alias;
        private Set<String>                                       entityAliases;
        private UpdateType                                        updateType;

        public Builder(
                String alias,
                Set<String> entityAliases,
                AssociationGroup.Builder builder,
                BuilderCallback<AssociationDefinition> builderCallback ) {

            super( builder, builderCallback );

            this.alias = alias;
            this.propertyDefinitionMap = Maps.newHashMap();
            this.entityAliases = entityAliases;
        }

        public Builder key( String... key ) {
            return key(
                    Stream.of( key ).map( FullQualifiedName::new ).toArray( FullQualifiedName[]::new ) );
        }

        public Builder key( FullQualifiedName... key ) {

            checkNotNull( key, "Key cannot be null." );
            checkArgument( ImmutableSet.copyOf( key ).size() == key.length, "Key must be a set of unique FQNs" );

            this.key = Arrays.asList( key );
            return this;
        }

        public Builder to( String entitySetName ) {

            this.entitySetName = entitySetName;
            return this;
        }

        public Builder ofType( String entityTypeFqn ) {
            return ofType( new FullQualifiedName( entityTypeFqn ) );
        }

        public Builder ofType( FullQualifiedName entityTypeFqn ) {
            this.entityTypeFqn = entityTypeFqn;
            return this;
        }

        public Builder fromEntity( String srcAlias ) {
            checkArgument(
                    entityAliases.contains( srcAlias ),
                    "The source entity must be a previously defined alias." );

            this.srcAlias = srcAlias;
            return this;
        }

        public Builder toEntity( String dstAlias ) {
            checkArgument(
                    entityAliases.contains( dstAlias ),
                    "The destination entity must be a previously defined alias." );

            this.dstAlias = dstAlias;
            return this;
        }

        public Builder entityIdGenerator( SerializableFunction<Map<String, Object>, String> generator ) {
            this.generator = generator;
            return this;
        }

        public Builder updateType( UpdateType updateType ) {
            this.updateType = updateType;
            return this;
        }

        public PropertyDefinition.Builder<AssociationDefinition.Builder> addProperty( String propertyTypeFqn ) {
            return addProperty( new FullQualifiedName( propertyTypeFqn ) );
        }

        public PropertyDefinition.Builder<AssociationDefinition.Builder> addProperty(
                FullQualifiedName propertyTypeFqn ) {

            BuilderCallback<PropertyDefinition> onBuild = propertyDefinition -> {
                FullQualifiedName propertyDefFqn = propertyDefinition.getFullQualifiedName();
                if ( propertyDefinitionMap.containsKey( propertyDefFqn ) ) {
                    throw new IllegalStateException(
                            String.format( "encountered duplicate property: %s", propertyDefFqn ) );
                }
                propertyDefinitionMap.put( propertyDefFqn, propertyDefinition );
            };

            return new PropertyDefinition.Builder<>( propertyTypeFqn, this, onBuild );
        }

        public Builder addProperty( String propertyString, String columnName ) {
            FullQualifiedName propertyFqn = new FullQualifiedName( propertyString );
            SerializableFunction<Map<String, Object>, ?> defaultMapper = row -> row.get( columnName );
            PropertyDefinition propertyDefinition = new PropertyDefinition(
                    propertyString, columnName, defaultMapper );
            this.propertyDefinitionMap.put( propertyFqn, propertyDefinition );
            return this;
        }

        public Builder addProperty(
                String propertyString,
                String columnName,
                List<Transformation> transformation,
                List<String> arguments ) {
            FullQualifiedName propertyFqn = new FullQualifiedName( propertyString );
            PropertyDefinition propertyDefinition =
                    new PropertyDefinition( propertyString,
                            columnName,
                            Optional.empty(),
                            Optional.empty(),
                            Optional.of( transformation ) );
            this.propertyDefinitionMap.put( propertyFqn, propertyDefinition );
            return this;
        }

        public AssociationGroup.Builder endAssociation() {
            if ( this.propertyDefinitionMap.size() == 0 ) {
                throw new IllegalStateException( "invoking addProperty() at least once is required" );
            }

            return super.ok( new AssociationDefinition( this ) );
        }
    }
}
