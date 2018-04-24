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

import java.io.Serializable;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;

import com.openlattice.search.requests.EntityKeyIdSearchResult;
import com.openlattice.search.requests.SearchDetails;
import com.openlattice.search.requests.SearchResult;

public class AdvancedSearchEntitySetDataLambda
        implements Function<ConductorElasticsearchApi, EntityKeyIdSearchResult>, Serializable {
    private static final long serialVersionUID = 8549826561713602245L;

    private UUID                entitySetId;
    private List<SearchDetails> searches;
    private int                 start;
    private int                 maxHits;
    private Set<UUID>           authorizedPropertyTypes;

    public AdvancedSearchEntitySetDataLambda(
            UUID entitySetId,
            List<SearchDetails> searches,
            int start,
            int maxHits,
            Set<UUID> authorizedPropertyTypes ) {
        this.entitySetId = entitySetId;
        this.searches = searches;
        this.start = start;
        this.maxHits = maxHits;
        this.authorizedPropertyTypes = authorizedPropertyTypes;
    }

    @Override
    public EntityKeyIdSearchResult apply( ConductorElasticsearchApi api ) {
        return api.executeAdvancedEntitySetDataSearch( entitySetId,
                searches,
                start,
                maxHits,
                authorizedPropertyTypes );
    }

}
