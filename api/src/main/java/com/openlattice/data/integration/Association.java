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

package com.openlattice.data.integration;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.openlattice.client.serialization.SerializationConstants;
import com.openlattice.data.EntityKey;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

public class Association {
    private final EntityKey              key;
    private final EntityKey              src;
    private final EntityKey              dst;

    @JsonCreator
    public Association(
            @JsonProperty( SerializationConstants.KEY_FIELD ) EntityKey key,
            @JsonProperty( SerializationConstants.SRC ) EntityKey src,
            @JsonProperty( SerializationConstants.DST ) EntityKey dst ) {
        this.key = key;
        this.src = src;
        this.dst = dst;
    }

    public Association( EntityKey key, EntityKey src, EntityKey dst, SetMultimap<UUID, Object> details ) {
        this( key, src, dst );
    }

    @JsonProperty( SerializationConstants.KEY_FIELD )
    public EntityKey getKey() {
        return key;
    }

    @JsonProperty( SerializationConstants.SRC )
    public EntityKey getSrc() {
        return src;
    }

    @JsonProperty( SerializationConstants.DST )
    public EntityKey getDst() {
        return dst;
    }

    @Override
    public String toString() {
        return "Association{" +
                "key=" + key +
                ", src=" + src +
                ", dst=" + dst +
                '}';
    }

    @Override
    public boolean equals( Object o ) {
        if ( this == o )
            return true;
        if ( o == null || getClass() != o.getClass() )
            return false;
        Association that = (Association) o;
        return Objects.equals( key, that.key ) &&
                Objects.equals( src, that.src ) &&
                Objects.equals( dst, that.dst );
    }

    @Override
    public int hashCode() {
        return Objects.hash( key, src, dst );
    }
}
