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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.openlattice.client.serialization.SerializationConstants;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class EnumType extends PropertyType {
    private static final EnumSet<EdmPrimitiveTypeKind> ALLOWED_UNDERLYING_TYPES = EnumSet.of(
            EdmPrimitiveTypeKind.Byte,
            EdmPrimitiveTypeKind.SByte,
            EdmPrimitiveTypeKind.Int16,
            EdmPrimitiveTypeKind.Int32,
            EdmPrimitiveTypeKind.Int64 );

    private final LinkedHashSet<String> members;
    private final boolean               flags;

    @JsonCreator
    public EnumType(
            @JsonProperty( SerializationConstants.ID_FIELD ) Optional<UUID> id,
            @JsonProperty( SerializationConstants.TYPE_FIELD ) FullQualifiedName fqn,
            @JsonProperty( SerializationConstants.TITLE_FIELD ) String title,
            @JsonProperty( SerializationConstants.DESCRIPTION_FIELD ) Optional<String> description,
            @JsonProperty( SerializationConstants.MEMBERS_FIELD ) LinkedHashSet<String> members,
            @JsonProperty( SerializationConstants.SCHEMAS ) Set<FullQualifiedName> schemas,
            @JsonProperty( SerializationConstants.DATATYPE_FIELD ) Optional<EdmPrimitiveTypeKind> datatype,
            @JsonProperty( SerializationConstants.FLAGS_FIELD ) boolean flags,
            @JsonProperty( SerializationConstants.PII_FIELD ) Optional<Boolean> piiField,
            @JsonProperty( SerializationConstants.MULTI_VALUED ) Optional<Boolean> multiValued,
            @JsonProperty( SerializationConstants.ANALYZER ) Optional<Analyzer> analyzer,
            @JsonProperty( SerializationConstants.INDEXED ) Optional<Boolean> postgresIndexed ) {
        super(
                id,
                fqn,
                title,
                description,
                schemas,
                datatype.orElse( EdmPrimitiveTypeKind.Int32 ),
                piiField,
                multiValued,
                analyzer,
                postgresIndexed );
        Preconditions.checkState( ALLOWED_UNDERLYING_TYPES.contains( this.datatype ),
                "%s is not one of %s",
                this.datatype,
                ALLOWED_UNDERLYING_TYPES );
        this.members = Preconditions.checkNotNull( members, "Members cannot be null" );
        this.flags = flags;
    }

    @JsonProperty( SerializationConstants.MEMBERS_FIELD )
    public Set<String> getMembers() {
        return Collections.unmodifiableSet( members );
    }

    @JsonProperty( SerializationConstants.FLAGS_FIELD )
    public boolean isFlags() {
        return flags;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ( flags ? 1231 : 1237 );
        result = prime * result + ( ( members == null ) ? 0 : members.hashCode() );
        return result;
    }

    @Override
    public boolean equals( Object obj ) {
        if ( this == obj ) {
            return true;
        }
        if ( !super.equals( obj ) ) {
            return false;
        }
        if ( !( obj instanceof EnumType ) ) {
            return false;
        }
        EnumType other = (EnumType) obj;
        if ( flags != other.flags ) {
            return false;
        }
        if ( members == null ) {
            if ( other.members != null ) {
                return false;
            }
        } else if ( !members.equals( other.members ) ) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "EnumType [members=" + members + ", flags=" + flags + ", datatype=" + datatype + ", piiField=" + piiField
                + ", analyzer=" + analyzer + ", schemas=" + schemas + ", type=" + type + ", id=" + id + ", title="
                + title + ", description=" + description + "]";
    }

}
