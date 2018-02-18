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

package com.openlattice.graph;

import java.util.UUID;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

public class EdgeSelection {
    private final Optional<UUID> optionalSrcId;
    private final Optional<UUID> optionalSrcType;
    private final Optional<UUID> optionalDstId;
    private final Optional<UUID> optionalDstType;
    private final Optional<UUID> optionalEdgeType;

    public EdgeSelection(
            Optional<UUID> optionalSrcId,
            Optional<UUID> optionalSrcType,
            Optional<UUID> optionalDstId,
            Optional<UUID> optionalDstType,
            Optional<UUID> optionalEdgeType ) {
        Preconditions.checkArgument(
                optionalSrcId.isPresent() || optionalSrcType.isPresent() || optionalDstId.isPresent()
                        || optionalDstType.isPresent() || optionalEdgeType.isPresent(),
                "You cannot run an empty edge selection query. At least one parameter must be specified." );
        this.optionalSrcId = optionalSrcId;
        this.optionalSrcType = optionalSrcType;
        this.optionalDstId = optionalDstId;
        this.optionalDstType = optionalDstType;
        this.optionalEdgeType = optionalEdgeType;
    }

    public Optional<UUID> getOptionalSrcId() {
        return optionalSrcId;
    }

    public Optional<UUID> getOptionalSrcType() {
        return optionalSrcType;
    }

    public Optional<UUID> getOptionalDstId() {
        return optionalDstId;
    }

    public Optional<UUID> getOptionalDstType() {
        return optionalDstType;
    }

    public Optional<UUID> getOptionalEdgeType() {
        return optionalEdgeType;
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ( ( optionalDstId == null ) ? 0 : optionalDstId.hashCode() );
        result = prime * result + ( ( optionalDstType == null ) ? 0 : optionalDstType.hashCode() );
        result = prime * result + ( ( optionalEdgeType == null ) ? 0 : optionalEdgeType.hashCode() );
        result = prime * result + ( ( optionalSrcId == null ) ? 0 : optionalSrcId.hashCode() );
        result = prime * result + ( ( optionalSrcType == null ) ? 0 : optionalSrcType.hashCode() );
        return result;
    }

    @Override
    public boolean equals( Object obj ) {
        if ( this == obj ) return true;
        if ( obj == null ) return false;
        if ( getClass() != obj.getClass() ) return false;
        EdgeSelection other = (EdgeSelection) obj;
        if ( optionalDstId == null ) {
            if ( other.optionalDstId != null ) return false;
        } else if ( !optionalDstId.equals( other.optionalDstId ) ) return false;
        if ( optionalDstType == null ) {
            if ( other.optionalDstType != null ) return false;
        } else if ( !optionalDstType.equals( other.optionalDstType ) ) return false;
        if ( optionalEdgeType == null ) {
            if ( other.optionalEdgeType != null ) return false;
        } else if ( !optionalEdgeType.equals( other.optionalEdgeType ) ) return false;
        if ( optionalSrcId == null ) {
            if ( other.optionalSrcId != null ) return false;
        } else if ( !optionalSrcId.equals( other.optionalSrcId ) ) return false;
        if ( optionalSrcType == null ) {
            if ( other.optionalSrcType != null ) return false;
        } else if ( !optionalSrcType.equals( other.optionalSrcType ) ) return false;
        return true;
    }

}
