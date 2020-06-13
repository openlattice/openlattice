package com.openlattice.scheduling

import com.hazelcast.core.HazelcastInstance
import com.hazelcast.core.IMap
import com.hazelcast.query.Predicates
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.scheduling.mapstores.ScheduledTasksMapstore
import com.openlattice.tasks.HazelcastFixedRateTask
import com.openlattice.tasks.Task
import org.slf4j.LoggerFactory
import java.time.Instant
import java.time.OffsetDateTime
import java.util.*
import java.util.concurrent.TimeUnit

private val logger = LoggerFactory.getLogger(ScheduledTaskService::class.java)
private const val RETRY_INTERVAL = 1_000L * 60 * 60 // 1 hour

class ScheduledTaskService : HazelcastFixedRateTask<ScheduledTaskServiceDependencies> {

    override fun getInitialDelay(): Long {
        return 1_000
    }

    override fun getPeriod(): Long {
        return 1_000
    }

    override fun getTimeUnit(): TimeUnit {
        return TimeUnit.MILLISECONDS
    }

    private fun tryLockTask(id: UUID, scheduledTaskLocks: IMap<UUID, Long>): Boolean {
        val currValue = scheduledTaskLocks.putIfAbsent(
                id,
                Instant.now().plusMillis(RETRY_INTERVAL).toEpochMilli(),
                RETRY_INTERVAL,
                TimeUnit.MILLISECONDS
        )
        return currValue == null
    }

    private fun executeScheduledTask(
            scheduledTask: ScheduledTask,
            scheduledTasks: IMap<UUID, ScheduledTask>,
            scheduledTaskLocks: IMap<UUID, Long>
    ) {
        if (tryLockTask(scheduledTask.id, scheduledTaskLocks)) {
            scheduledTask.task.run(getDependency().hazelcast)
            scheduledTasks.delete(scheduledTask.id)
        } else {
            logger.info("Skipping scheduled task ${scheduledTask.id} as it has already been attempted within the past hour.")
        }
    }

    override fun runTask() {
        val scheduledTasks = HazelcastMap.SCHEDULED_TASKS.getMap(getDependency().hazelcast)
        val scheduledTaskLocks = HazelcastMap.SCHEDULED_TASK_LOCKS.getMap(getDependency().hazelcast)

        scheduledTasks.values(Predicates.lessEqual(ScheduledTasksMapstore.SCHEDULED_DATE_TIME_INDEX, OffsetDateTime.now())).forEach {
            getDependency().executor.submit {
                try {
                    executeScheduledTask(it, scheduledTasks, scheduledTaskLocks)
                } catch (e: Exception) {
                    logger.error("Unable to run scheduled task with id ${it.id}", e)
                }
            }
        }
    }

    override fun getName(): String {
        return Task.SCHEDULED_TASK_SERVICE.name
    }

    override fun getDependenciesClass(): Class<out ScheduledTaskServiceDependencies> {
        return ScheduledTaskServiceDependencies::class.java
    }
}