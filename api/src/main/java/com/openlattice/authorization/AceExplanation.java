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

package com.openlattice.authorization;

import java.util.Set;

import com.openlattice.client.serialization.SerializationConstants;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class AceExplanation {
    private final Ace      ace;
    // Explains where the ace comes from.
    private final Set<Ace> explanation;
    private transient int  h = 0;

    @JsonCreator
    public AceExplanation( 
            @JsonProperty( SerializationConstants.ACE ) Ace ace, 
            @JsonProperty( SerializationConstants.EXPLANATION ) Set<Ace> explanation ) {
        this.ace = ace;
        this.explanation = explanation;
    }

    @JsonProperty( SerializationConstants.ACE )
    public Ace getAce() {
        return ace;
    }

    @JsonProperty( SerializationConstants.EXPLANATION )
    public Set<Ace> getExplanation() {
        return explanation;
    }

    public void addExplanation( Ace ace ) {
        explanation.add( ace );
    }

    @Override
    public int hashCode() {
        if ( h == 0 ) {
            final int prime = 31;
            int result = 1;
            result = prime * result + ( ( ace == null ) ? 0 : ace.hashCode() );
            result = prime * result + ( ( explanation == null ) ? 0 : explanation.hashCode() );
            h = result;
        }
        return h;
    }

    @Override
    public boolean equals( Object obj ) {
        if ( this == obj ) return true;
        if ( obj == null ) return false;
        if ( getClass() != obj.getClass() ) return false;
        AceExplanation other = (AceExplanation) obj;
        if ( ace == null ) {
            if ( other.ace != null ) return false;
        } else if ( !ace.equals( other.ace ) ) return false;
        if ( explanation == null ) {
            if ( other.explanation != null ) return false;
        } else if ( !explanation.equals( other.explanation ) ) return false;
        return true;
    }

    @Override
    public String toString() {
        return "AceExplained [ace=" + ace + ", explanation=" + explanation + "]";
    }

}
