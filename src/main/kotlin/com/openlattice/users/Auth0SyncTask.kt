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
import com.hazelcast.scheduledexecutor.StatefulTask
import com.openlattice.tasks.HazelcastFixedRateTask
import com.openlattice.tasks.HazelcastTaskDependencies
import com.openlattice.tasks.Task
import org.slf4j.LoggerFactory
import java.time.Instant
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit

const val REFRESH_INTERVAL_MILLIS = 120_000L // 2 min
private const val MAX_JOBS = 8
private const val LAST_SYNC = "lastSync"
private val logger = LoggerFactory.getLogger(Auth0SyncTask::class.java)

/**
 * This is the auth0 synchronization task that runs every REFRESH_INTERVAL_MILLIS in Hazelcast. It requires that
 * syncDependencies be initialized within the same JVM in order to function properly.
 */
class Auth0SyncTask
    : HazelcastFixedRateTask<Auth0SyncTaskDependencies>, HazelcastTaskDependencies, StatefulTask<String, Instant> {

    private val syncSemaphore = Semaphore(MAX_JOBS)

    private var lastSync = Instant.now()

    override fun getDependenciesClass(): Class<Auth0SyncTaskDependencies> {
        return Auth0SyncTaskDependencies::class.java
    }

    override fun getName(): String {
        return Task.AUTH0_SYNC_TASK.name
    }

    override fun getInitialDelay(): Long {
        return REFRESH_INTERVAL_MILLIS
    }

    override fun getPeriod(): Long {
        return REFRESH_INTERVAL_MILLIS
    }

    override fun getTimeUnit(): TimeUnit {
        return TimeUnit.MILLISECONDS
    }

    private fun initialized(): Boolean {
        return getDependency().users.usersInitialized()
    }

    override fun runTask() {
        if (!initialized()) {
            logger.warn("Users not yet initialized.")
            return
        }

        updateUsersCache()
        syncUsers()
    }

    /**
     * Retrieves updated users from auth0 and adds them to hazelcast.
     */
    private fun updateUsersCache() {
        val ds = getDependency()
        logger.info("Updating users.")
        val currentSync = Instant.now()

        ds.userListingService
                .getUpdatedUsers(lastSync, currentSync)
                .map {
                    syncSemaphore.acquire()
                    ds.executor.submit {
                        try {
                            ds.users.updateUser(it)
                        } catch (ex: Exception) {
                            logger.error("Unable to update user ${it.id}", ex)
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
    private fun syncUsers() {
        val ds = getDependency()
        logger.info("Synchronizing users.")

        ds.users.getCachedUsers()
                .map {
                    syncSemaphore.acquire()
                    ds.executor.submit {
                        try {
                            ds.users.syncUserEnrollmentsAndAuthentication(it)
                        } catch (ex: Exception) {
                            logger.error("Unable to synchronize enrollments and permissions for user ${it.id}", ex)
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
    fun initializeUsers() {
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

