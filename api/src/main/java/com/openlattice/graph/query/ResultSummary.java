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

package com.openlattice.graph.query;

import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class ResultSummary {
    private final UUID           queryId;
    private final long           entityCount;
    private final long           edgeCount;
    private final OffsetDateTime expiration;

    public ResultSummary( UUID queryId, long entityCount, long edgeCount, OffsetDateTime expiration ) {
        this.queryId = queryId;
        this.entityCount = entityCount;
        this.edgeCount = edgeCount;
        this.expiration = expiration;
    }
}
