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

package com.openlattice.edm.type;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.openlattice.authorization.securable.AbstractSchemaAssociatedSecurableType;
import com.openlattice.authorization.securable.SecurableObjectType;
import com.openlattice.client.serialization.SerializationConstants;
import com.openlattice.postgres.IndexType;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class PropertyType extends AbstractSchemaAssociatedSecurableType {
    private static final EnumSet<EdmPrimitiveTypeKind> ALLOWED_UNDERLYING_TYPES = EnumSet.of(
            EdmPrimitiveTypeKind.Byte,
            EdmPrimitiveTypeKind.SByte,
            EdmPrimitiveTypeKind.Int16,
            EdmPrimitiveTypeKind.Int32,
            EdmPrimitiveTypeKind.Int64,
            EdmPrimitiveTypeKind.String );

    protected final   boolean               multiValued;
    private final     LinkedHashSet<String> enumValues = new LinkedHashSet<>();
    protected         EdmPrimitiveTypeKind  datatype;
    protected         boolean               piiField;
    protected         Analyzer              analyzer;
    protected         IndexType             postgresIndexType;
    private transient int                   h          = 0;

    @JsonCreator
    public PropertyType(
            @JsonProperty( SerializationConstants.ID_FIELD ) Optional<UUID> id,
            @JsonProperty( SerializationConstants.TYPE_FIELD ) FullQualifiedName fqn,
            @JsonProperty( SerializationConstants.TITLE_FIELD ) String title,
            @JsonProperty( SerializationConstants.DESCRIPTION_FIELD ) Optional<String> description,
            @JsonProperty( SerializationConstants.SCHEMAS ) Set<FullQualifiedName> schemas,
            @JsonProperty( SerializationConstants.DATATYPE_FIELD ) EdmPrimitiveTypeKind datatype,
            @JsonProperty( SerializationConstants.ENUM_VALUES ) Optional<Set<String>> enumValues,
            @JsonProperty( SerializationConstants.PII_FIELD ) Optional<Boolean> piiField,
            @JsonProperty( SerializationConstants.MULTI_VALUED ) Optional<Boolean> multiValued,
            @JsonProperty( SerializationConstants.ANALYZER ) Optional<Analyzer> analyzer,
            @JsonProperty( SerializationConstants.INDEX_TYPE ) Optional<IndexType> postgresIndexType ) {
        super(
                id,
                fqn,
                title,
                description,
                schemas );
        this.datatype = Preconditions.checkNotNull( datatype, "PropertyType datatype cannot be null" );
        if ( enumValues.isPresent() ) {
            if ( enumValues.get().size() > 0 ) {
                checkArgument( ALLOWED_UNDERLYING_TYPES.contains( datatype ), "Only certain types are allowed" );
                this.enumValues.addAll( enumValues.get() );
            }
        }
        this.piiField = piiField.orElse( false );
        this.multiValued = multiValued.orElse( true );
        this.analyzer = analyzer.orElse( Analyzer.STANDARD );

        if ( EdmPrimitiveTypeKind.Binary == this.datatype ) {
            this.postgresIndexType = postgresIndexType.orElse( IndexType.NONE );
            checkArgument( this.postgresIndexType == IndexType.NONE, "Indexes are not allowed on Binary datatypes" );
        } else {
            this.postgresIndexType = postgresIndexType.orElse( IndexType.BTREE );
        }
    }

    public PropertyType(
            UUID id,
            FullQualifiedName fqn,
            String title,
            Optional<String> description,
            Set<FullQualifiedName> schemas,
            EdmPrimitiveTypeKind datatype,
            Optional<Set<String>> enumValues,
            Optional<Boolean> piiField,
            Optional<Analyzer> analyzer,
            Optional<IndexType> postgresIndexType ) {
        this( Optional.of( id ),
                fqn,
                title,
                description,
                schemas,
                datatype,
                enumValues,
                piiField,
                Optional.empty(),
                analyzer,
                postgresIndexType );
    }

    public PropertyType(
            UUID id,
            FullQualifiedName fqn,
            String title,
            Optional<String> description,
            Set<FullQualifiedName> schemas,
            EdmPrimitiveTypeKind datatype,
            Optional<Boolean> piiField,
            Optional<Analyzer> analyzer,
            Optional<IndexType> postgresIndexType ) {
        this( Optional.of( id ),
                fqn,
                title,
                description,
                schemas,
                datatype,
                Optional.empty(),
                piiField,
                Optional.empty(),
                analyzer,
                postgresIndexType );
    }

    public PropertyType(
            UUID id,
            FullQualifiedName fqn,
            String title,
            Optional<String> description,
            Set<FullQualifiedName> schemas,
            EdmPrimitiveTypeKind datatype,
            Optional<Boolean> piiField ) {
        this( Optional.of( id ),
                fqn,
                title,
                description,
                schemas,
                datatype,
                Optional.empty(),
                piiField,
                Optional.empty(),
                Optional.empty(),
                Optional.empty() );
    }

    public PropertyType(
            UUID id,
            FullQualifiedName fqn,
            String title,
            Optional<String> description,
            Set<FullQualifiedName> schemas,
            EdmPrimitiveTypeKind datatype ) {
        this( Optional.of( id ),
                fqn,
                title,
                description,
                schemas,
                datatype,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty() );
    }

    public PropertyType(
            FullQualifiedName fqn,
            String title,
            Optional<String> description,
            Set<FullQualifiedName> schemas,
            EdmPrimitiveTypeKind datatype ) {
        this( Optional.empty(),
                fqn,
                title,
                description,
                schemas,
                datatype,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty() );
    }

    @JsonProperty( SerializationConstants.DATATYPE_FIELD )
    public EdmPrimitiveTypeKind getDatatype() {
        return datatype;
    }

    @JsonProperty( SerializationConstants.PII_FIELD )
    public boolean isPii() {
        return piiField;
    }

    @JsonIgnore
    public void setPii( boolean pii ) {
        piiField = pii;
    }

    @JsonProperty( SerializationConstants.MULTI_VALUED )
    public boolean isMultiValued() {
        return multiValued;
    }

    @JsonProperty( SerializationConstants.ANALYZER )
    public Analyzer getAnalyzer() {
        return analyzer;
    }

    @JsonProperty( SerializationConstants.INDEX_TYPE )
    public IndexType getPostgresIndexType() {
        return postgresIndexType;
    }

    @JsonProperty( SerializationConstants.ENUM_VALUES )
    @JsonInclude( value = Include.NON_EMPTY )
    public LinkedHashSet<String> getEnumValues() {
        return enumValues;
    }

    @JsonIgnore
    public void setPostgresIndexType( IndexType postgresIndexType ) {
        this.postgresIndexType = postgresIndexType;
    }

    @Override
    public boolean equals( Object o ) {
        if ( this == o ) {
            return true;
        }
        if ( o == null || getClass() != o.getClass() ) {
            return false;
        }
        if ( !super.equals( o ) ) {
            return false;
        }
        PropertyType that = (PropertyType) o;
        return hashCode() == that.hashCode() &&
                multiValued == that.multiValued &&
                piiField == that.piiField &&
                Objects.equals( enumValues, that.enumValues ) &&
                datatype == that.datatype &&
                analyzer == that.analyzer &&
                postgresIndexType == that.postgresIndexType;
    }

    @Override
    public int hashCode() {
        if ( h == 0 ) {
            h = Objects.hash( super.hashCode(), multiValued, piiField, enumValues, datatype, analyzer, postgresIndexType );
        }
        return h;
    }

    @Override
    public String toString() {
        return "PropertyType [datatype=" + datatype + ", piiField=" + piiField + ", analyzer=" + analyzer + ", schemas="
                + schemas + ", type=" + type + ", id=" + id + ", title=" + title + ", description=" + description + "]";
    }

    @Override
    @JsonIgnore
    public SecurableObjectType getCategory() {
        return SecurableObjectType.PropertyTypeInEntitySet;
    }
}
