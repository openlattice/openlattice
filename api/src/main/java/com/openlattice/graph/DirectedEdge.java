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

import com.openlattice.data.EntityKey;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class DirectedEdge {
    private final EntityKey src;
    private final EntityKey dst;

    public DirectedEdge( EntityKey src, EntityKey dst ) {
        this.src = src;
        this.dst = dst;
    }

    public EntityKey getSource() {
        return src;
    }

    public EntityKey getDestination() {
        return dst;
    }
}
