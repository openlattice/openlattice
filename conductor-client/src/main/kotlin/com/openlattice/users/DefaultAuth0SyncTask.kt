/*
 * Copyright (C) 2017. OpenLattice, Inc.
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
 */

package com.openlattice.users


import com.google.common.base.Stopwatch
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Instant
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit

private const val MAX_JOBS = 8

/**
 * This is the auth0 synchronization task that runs every REFRESH_INTERVAL_MILLIS in Hazelcast. It requires that
 * syncDependencies be initialized within the same JVM in order to function properly.
 */
class DefaultAuth0SyncTask: Auth0SyncTask {
    override val isLocal: Boolean = false

    private val syncSemaphore = Semaphore(MAX_JOBS)

    override val logger: Logger = LoggerFactory.getLogger(DefaultAuth0SyncTask::class.java)

    override var lastSync: Instant = Instant.now()

    override fun initialized(): Boolean {
        return getDependency().users.usersInitialized()
    }

    /**
     * Retrieves updated users from auth0 and adds them to hazelcast.
     */
    override fun updateUsersCache() {
        val deps = getDependency()
        logger.info("Updating users.")
        val currentSync = Instant.now()

        deps.userListingService.getUpdatedUsers(lastSync, currentSync)
                .chunked(DEFAULT_CHUNK_SIZE)
                .map {
                    syncSemaphore.acquire()
                    deps.executor.submit {
                        try {
                            deps.users.createOrUpdateUsers(it)
                        } catch (ex: Exception) {
                            logger.error("Unable to update users $it", ex)
                        } finally {
                            syncSemaphore.release()
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

    /**
     * Synchronizes all cached users.
     */
    override fun syncUsers() {
        val ds = getDependency()
        logger.info("Synchronizing users.")

        ds.users.getCachedUsers()
                .chunked(DEFAULT_CHUNK_SIZE)
                .map {
                    syncSemaphore.acquire()

                    val userIds = it.map { user -> user.id }.toSet()

                    ds.executor.submit {
                        try {
                            ds.users.syncAuthenticationCacheForPrincipalIds(userIds)
                        } catch (ex: Exception) {
                            logger.error("Unable to synchronize enrollments and permissions for users $userIds", ex)
                        } finally {
                            syncSemaphore.release()
                        }
                    }
                }
                // we want to materialize the list of futures so the work happens in the background.
                .toList()
                .forEach {
                    it.get()
                }
    }

    /**
     * Loads and synchronizes all users from auth0.
     */
    override fun initializeUsers() {
        if (initialized()) {
            return
        }

        logger.info("Initial synchronization of users started.")
        val sw = Stopwatch.createStarted()

        val ds = getDependency()
        ds.userListingService
                .getAllUsers()
                .map {
                    syncSemaphore.acquire()
                    ds.executor.submit {
                        try {
                            ds.users.syncUser(it)
                        } catch (ex: Exception) {
                            logger.error("Unable to initially synchronize user ${it.id}", ex)
                        } finally {
                            syncSemaphore.release()
                        }
                    }
                }
                // we want to materialize the list of futures so the work happens in the background.
                .toList()
                .forEach {
                    it.get()
                }

        logger.info("Finished initializing all users in ${sw.elapsed(TimeUnit.MILLISECONDS)} ms.")
    }

    override fun save(snapshot: MutableMap<String, Instant>) {
        snapshot[LAST_SYNC] = lastSync
    }

    override fun load(snapshot: MutableMap<String, Instant>) {
        if (snapshot.containsKey(LAST_SYNC)) {
            lastSync = snapshot.getValue(LAST_SYNC)
        }
    }
}
