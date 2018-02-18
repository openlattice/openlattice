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

package com.openlattice.edm.set;

import com.openlattice.client.serialization.SerializationConstants;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class EntitySetPropertyMetadata {

    private String  title;
    private String  description;
    private boolean defaultShow;

    @JsonCreator
    public EntitySetPropertyMetadata(
            @JsonProperty( SerializationConstants.TITLE_FIELD ) String title,
            @JsonProperty( SerializationConstants.DESCRIPTION_FIELD ) String description,
            @JsonProperty( SerializationConstants.DEFAULT_SHOW ) boolean defaultShow ) {
        this.title = title;
        this.description = description;
        this.defaultShow = defaultShow;
    }

    @JsonProperty( SerializationConstants.TITLE_FIELD )
    public String getTitle() {
        return title;
    }

    @JsonProperty( SerializationConstants.DESCRIPTION_FIELD )
    public String getDescription() {
        return description;
    }

    @JsonProperty( SerializationConstants.DEFAULT_SHOW )
    public boolean getDefaultShow() {
        return defaultShow;
    }
    
    @JsonIgnore
    public void setTitle( String title ) {
        this.title = title;
    }
    
    @JsonIgnore
    public void setDescription( String description ) {
        this.description = description;
    }
    
    @JsonIgnore
    public void setDefaultShow( boolean defaultShow ) {
        this.defaultShow = defaultShow;
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ( defaultShow ? 1231 : 1237 );
        result = prime * result + ( ( description == null ) ? 0 : description.hashCode() );
        result = prime * result + ( ( title == null ) ? 0 : title.hashCode() );
        return result;
    }

    @Override
    public boolean equals( Object obj ) {
        if ( this == obj ) return true;
        if ( obj == null ) return false;
        if ( getClass() != obj.getClass() ) return false;
        EntitySetPropertyMetadata other = (EntitySetPropertyMetadata) obj;
        if ( defaultShow != other.defaultShow ) return false;
        if ( description == null ) {
            if ( other.description != null ) return false;
        } else if ( !description.equals( other.description ) ) return false;
        if ( title == null ) {
            if ( other.title != null ) return false;
        } else if ( !title.equals( other.title ) ) return false;
        return true;
    }

}
