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
package com.openlattice.users

import com.google.common.base.Stopwatch
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Instant
import java.util.concurrent.TimeUnit

/**
 * @author Drew Bailey (drew@openlattice.com)
 */
class LocalAuth0SyncTask: Auth0SyncTask {
    override val isLocal: Boolean = true

    override val logger: Logger = LoggerFactory.getLogger(LocalAuth0SyncTask::class.java)

    override var lastSync: Instant = Instant.now()

    override fun initialized(): Boolean {
        return getDependency().users.usersInitialized()
    }

    override fun initializeUsers() {
        if (initialized()) {
            return
        }

        logger.info("Initial synchronization of users started.")
        val sw = Stopwatch.createStarted()

        val ds = getDependency()
        ds.users.syncUsers(ds.userListingService.getAllUsers())
        logger.info("Finished initializing all users in ${sw.elapsed(TimeUnit.MILLISECONDS)} ms.")
    }

    override fun syncUsers() {
        val ds = getDependency()
        logger.info("Synchronizing users.")

        ds.users.getCachedUsers()
                .chunked(DEFAULT_CHUNK_SIZE)
                .map {
                    val userIds = it.map { user -> user.id }.toSet()

                    ds.executor.submit {
                        try {
                            ds.users.syncAuthenticationCacheForPrincipalIds(userIds)
                        } catch (ex: Exception) {
                            logger.error("Unable to synchronize enrollments and permissions for users $userIds", ex)
                        }
                    }
                }
                // we want to materialize the list of futures so the work happens in the background.
                .toList()
                .forEach {
                    it.get()

                }
    }

    override fun updateUsersCache() {
        val deps = getDependency()
        logger.info("Updating users.")
        val currentSync = Instant.now()

        deps.userListingService.getUpdatedUsers(lastSync, currentSync)
                .chunked(DEFAULT_CHUNK_SIZE)
                .map {
                    deps.executor.submit {
                        try {
                            deps.users.createOrUpdateUsers(it)
                        } catch (ex: Exception) {
                            logger.error("Unable to update users $it", ex)
                        }
                    }
                }
                // we want to materialize the list of futures so the work happens in the background.
                .toList()
                .forEach {
                    it.get()
                }
        lastSync = currentSync
    }

}