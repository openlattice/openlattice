/*
 * Copyright (C) 2020. OpenLattice, Inc.
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

package com.openlattice.hazelcast

import com.hazelcast.core.HazelcastInstance
import com.hazelcast.core.IQueue
import com.openlattice.auditing.AuditableEvent
import com.openlattice.codex.MessageRequest
import com.openlattice.data.EntityDataKey
import com.openlattice.edm.EntitySet
import com.openlattice.mail.RenderableEmailRequest
import java.time.OffsetDateTime
import java.util.*

class HazelcastQueue<T> internal constructor(val name: String) : TypedQueueIdentifier<T> {
    private val checker = instanceChecker.checkInstance(name)

    override fun name(): String {
        this.checker.check()
        return name
    }

    override fun toString(): String {
        return name
    }

    fun getQueue(hazelcast: HazelcastInstance): IQueue<T> {
        this.checker.check()
        return hazelcast.getQueue(name)
    }

    companion object {
        private val instanceChecker = UniqueInstanceManager(HazelcastQueue::class.java)

        // When adding new entries to this list, please make sure to keep it sorted and keep the name in sync
        @JvmField val AUDITING = HazelcastQueue<AuditableEvent>("AUDITING")
        @JvmField val BACKGROUND_INDEXING  = HazelcastQueue<EntitySet>("BACKGROUND_INDEXING")
        @JvmField val EMAIL_SPOOL  = HazelcastQueue<RenderableEmailRequest>("EMAIL_SPOOL")
        @JvmField val ID_GENERATION  = HazelcastQueue<UUID>("ID_GENERATION")
        @JvmField val INDEXING = HazelcastQueue<UUID>("INDEXING")
        @JvmField val LINKING_CANDIDATES = HazelcastQueue<EntityDataKey>("LINKING_CANDIDATES")
        @JvmField val LINKING_INDEXING = HazelcastQueue<Triple<List<Array<UUID>>, UUID, OffsetDateTime>>("LINKING_INDEXING")
        @JvmField val LINKING_UNINDEXING = HazelcastQueue<Triple<List<Array<UUID>>, UUID, OffsetDateTime>>("LINKING_UNINDEXING")
        @JvmField val TWILIO = HazelcastQueue<MessageRequest>("TWILIO")
    }
}
