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

package com.openlattice.authorization.securable;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.openlattice.client.serialization.SerializationConstants;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;

import java.util.UUID;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Base class for all securable objects in the system.
 */
@JsonInclude(value= Include.NON_ABSENT) //This means empty collections will not be included in generated JSON.
public abstract class AbstractSecurableObject {
    protected final UUID    id;
    //This is only a descriptive property so relax finality.
    protected String  title;
    protected String  description;
    
    private final   boolean idPresent;

    /**
     * @param id          The UUID of the securable object. Must not be null.
     * @param title       The title of the securable object. Must not be blank.
     * @param description An optional description for the object. Can be blank or null.
     */
    protected AbstractSecurableObject(
            UUID id,
            String title,
            Optional<String> description ) {
        this( id, title, description, true );
    }

    /**
     * @param id          An optional id for the securable object in the form a UUID.
     * @param title       The title of the securable object. Must not be blank.
     * @param description An optional description for the object. Can be blank or null.
     */
    protected AbstractSecurableObject(
            Optional<UUID> id,
            String title,
            Optional<String> description ) {
        this( id.orElseGet( UUID::randomUUID ), title, description, id.isPresent() );
    }

    /**
     * @param id          The id of the securable object in the form of a UUID. Must not be null.
     * @param title       The title of the securable object. Must not be blank.
     * @param description An optional description for the object. Can be blank or null.
     * @param idPresent   Whether the id was present at creation time or whether it was generate randomly.
     */
    private AbstractSecurableObject(
            UUID id,
            String title,
            Optional<String> description,
            boolean idPresent ) {

        /*
         * There is no logical requirement that the title not be blank, it would just be very confusing to have a bunch
         * of organizations with no title whatsoever. This can be relaxed in the future.
         */
        checkArgument( StringUtils.isNotBlank( title ), "Title cannot be blank." );
        this.id = checkNotNull( id );
        this.idPresent = idPresent;
        this.description = description.orElse( "" );
        this.title = title;
    }

    @JsonProperty( SerializationConstants.ID_FIELD )
    public UUID getId() {
        return id;
    }

    @JsonProperty( SerializationConstants.TITLE_FIELD )
    public String getTitle() {
        return title;
    }

    @Override
    public String toString() {
        return "AbstractSecurableObject [id=" + id + ", title=" + title + ", description=" + description
                + ", idPresent=" + idPresent + "]";
    }

    @JsonProperty( SerializationConstants.DESCRIPTION_FIELD )
    public String getDescription() {
        return description;
    }

    @JsonIgnore
    public void setTitle( String title ){
        this.title = title;
    }
    
    @JsonIgnore    
    public void setDescription( String description ){
        this.description = description;
    }
    
    @JsonIgnore
    public boolean wasIdPresent() {
        return idPresent;
    }

    public abstract SecurableObjectType getCategory();

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ( ( id == null ) ? 0 : id.hashCode() );
        result = prime * result + ( ( description == null ) ? 0 : description.hashCode() );
        result = prime * result + ( idPresent ? 1231 : 1237 );
        result = prime * result + ( ( title == null ) ? 0 : title.hashCode() );
        return result;
    }

    @Override
    public boolean equals( Object obj ) {
        if ( this == obj ) {
            return true;
        }
        if ( obj == null ) {
            return false;
        }
        if ( !( obj instanceof AbstractSecurableObject ) ) {
            return false;
        }
        AbstractSecurableObject other = (AbstractSecurableObject) obj;
        if ( id == null ) {
            if ( other.id != null ) {
                return false;
            }
        } else if ( !id.equals( other.id ) ) {
            return false;
        }
        if ( description == null ) {
            if ( other.description != null ) {
                return false;
            }
        } else if ( !description.equals( other.description ) ) {
            return false;
        }
        if ( title == null ) {
            if ( other.title != null ) {
                return false;
            }
        } else if ( !title.equals( other.title ) ) {
            return false;
        }
        return true;
    }

}
