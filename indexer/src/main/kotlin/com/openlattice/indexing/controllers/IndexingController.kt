/*
 * Copyright (C) 2019. OpenLattice, Inc.
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

package com.openlattice.indexing.controllers

import com.openlattice.admin.indexing.CONTROLLER
import com.openlattice.admin.indexing.IndexAdminApi
import com.openlattice.admin.indexing.IndexingState
import com.openlattice.admin.indexing.REINDEX
import com.openlattice.authorization.AuthorizationManager
import com.openlattice.authorization.AuthorizingComponent
import com.openlattice.indexing.IndexingService
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.*
import java.util.*
import javax.inject.Inject

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@RestController
@RequestMapping(CONTROLLER)
class IndexingController : IndexAdminApi, AuthorizingComponent {
    @Inject
    private lateinit var indexingService: IndexingService

    @Inject
    private lateinit var authorizationManager: AuthorizationManager

    override fun getAuthorizationManager(): AuthorizationManager {
        return authorizationManager
    }

    @GetMapping(value = [REINDEX], produces = [MediaType.APPLICATION_JSON_VALUE])
    override fun getIndexingState(): IndexingState {
        ensureAdminAccess()
        return indexingService.getIndexingState()
    }

    @PostMapping(
            value = [REINDEX], consumes = [MediaType.APPLICATION_JSON_VALUE],
            produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun reindex(reindexRequest: Map<UUID, Set<UUID>>): IndexingState {
        ensureAdminAccess()
        indexingService.queueForIndexing(reindexRequest)
        return indexingService.getIndexingState()
    }

    @PutMapping(
            value = [REINDEX], consumes = [MediaType.APPLICATION_JSON_VALUE],
            produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun updateReindex(reindexRequest: Map<UUID, Set<UUID>>): IndexingState {
        ensureAdminAccess()
        indexingService.setForIndexing(reindexRequest)
        return indexingService.getIndexingState()
    }

    @DeleteMapping(
            value = [REINDEX], produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun clearIndexingQueue(): Int {
        ensureAdminAccess()
        return indexingService.clearIndexingJobs()
    }


}