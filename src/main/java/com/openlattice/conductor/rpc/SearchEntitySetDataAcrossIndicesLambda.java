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
 */

package com.openlattice.conductor.rpc;

import com.openlattice.rhizome.hazelcast.DelegatedStringSet;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

public class SearchEntitySetDataAcrossIndicesLambda
        implements Function<ConductorElasticsearchApi, List<UUID>>, Serializable {

    private static final long serialVersionUID = 874720830583573161L;

    private Iterable<UUID>               entitySetIds;
    @SuppressFBWarnings( value = "SE_BAD_FIELD", justification = "Custom Stream Serializer is implemented" )
    private Map<UUID, DelegatedStringSet> fieldSearches;
    private int                           size;
    private boolean                       explain;

    public SearchEntitySetDataAcrossIndicesLambda(
            Iterable<UUID>  entitySetIds,
            Map<UUID, DelegatedStringSet> fieldSearches,
            int size,
            boolean explain ) {
        this.entitySetIds = entitySetIds;
        this.fieldSearches = fieldSearches;
        this.size = size;
        this.explain = explain;
    }

    @Override
    public List<UUID> apply( ConductorElasticsearchApi api ) {
        return api.executeEntitySetDataSearchAcrossIndices( entitySetIds, fieldSearches, size, explain );
    }
}
