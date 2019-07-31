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

import com.codahale.metrics.annotation.Timed
import com.dataloom.mappers.ObjectMappers
import org.slf4j.LoggerFactory

private val mapper = ObjectMappers.newJsonMapper()

/**
 *
 * This class makes it easy for other classes to implement auditing by passing a instance of the auditable event class
 * with the appropriate data configured.
 */

private val logger = LoggerFactory.getLogger(AuditingComponent::class.java)

interface AuditingComponent {

    companion object {
        const val MAX_ENTITY_KEY_IDS_PER_EVENT = 100
    }

    fun getAuditingManager() : AuditingManager


    @Timed
    @JvmDefault
    fun recordEvent(event: AuditableEvent): Int {
        return recordEvents(listOf(event))
    }

    @Timed
    @JvmDefault
    fun recordEvents(events: List<AuditableEvent>): Int {
        return getAuditingManager().recordEvents(events)
    }

}