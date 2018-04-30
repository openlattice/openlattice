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

package com.openlattice.graph;

import com.openlattice.graph.query.AbstractOp;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class EntityQuery extends AbstractOp {
    private final Set<UUID>      entitySetId;
    private final Optional<UUID> entityTypeId;

    public EntityQuery(
            Integer id,
            Set<Integer> ops,
            boolean negated,
            Set<UUID> entitySetId,
            Optional<UUID> entityTypeId ) {
        super( id, ops, negated );
        this.entitySetId = entitySetId;
        this.entityTypeId = entityTypeId;
    }

    public EntityQuery( Set<Integer> ops, Set<UUID> entitySetId, Optional<UUID> entityTypeId ) {
        super( ops, false );
        this.entitySetId = entitySetId;
        this.entityTypeId = entityTypeId;
    }
}
