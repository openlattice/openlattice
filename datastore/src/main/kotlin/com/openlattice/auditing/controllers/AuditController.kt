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

package com.openlattice.auditing.controllers

import com.openlattice.auditing.*
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.PutMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import java.util.*
import javax.inject.Inject

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@RestController
@RequestMapping(CONTROLLER)
class AuditController : AuditApi, AuditingComponent {

    @Inject
    private lateinit var auditingManager: AuditingManager

    override fun getAuditingManager(): AuditingManager {
        return auditingManager
    }

    override fun audit(audit: Audit): List<Map<UUID, Set<Any>>> {
        TODO("Audit entity sets can be searched using SearchApi directly")
    }

    @PutMapping(value = ["", "/"], consumes = [MediaType.APPLICATION_JSON_VALUE])
    override fun record(events: List<AuditableEvent>): Int {
        return recordEvents(events)
    }

    override fun auditPrincipal(auditPrincipal: AuditPrincipal): List<Map<UUID, Set<Any>>> {
        TODO("Audit entity sets can be searched using SearchApi directly")
    }

    override fun auditEntitySet(auditEntitySet: AuditEntitySet): List<Map<UUID, Set<Any>>> {
        TODO("Audit entity sets can be searched using SearchApi directly")
    }
}