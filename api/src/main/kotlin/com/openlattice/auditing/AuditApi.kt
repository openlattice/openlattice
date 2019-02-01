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

package com.openlattice.auditing

import retrofit2.http.GET
import retrofit2.http.POST
import retrofit2.http.PUT
import retrofit2.http.Path
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */

// @formatter:off
const val SERVICE = "/datastore"
const val CONTROLLER = "/audit"
const val BASE = SERVICE + CONTROLLER
// @formatter:on

const val ENTITY_SET = "/entity-set"
const val ID = "id"
const val ID_PATH = "/{$ID}"
const val PRINCIPAL = "principal"

interface AuditApi {
    /**
     * Retrieve all the events record for a particular entity set in a given time window.
     * @param auditEntitySet The
     */
    @POST(BASE + ENTITY_SET)
    fun auditEntitySet(auditEntitySet: AuditEntitySet): List<Map<UUID, Set<Any>>>

    @POST(BASE + PRINCIPAL)
    fun auditPrincipal(auditPrincipal: AuditPrincipal): List<Map<UUID, Set<Any>>>

    @POST(BASE)
    fun audit(audit: Audit): List<Map<UUID, Set<Any>>>

    @PUT(BASE)
    fun record(events: List<AuditableEvent>): Int
}