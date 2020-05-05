package com.openlattice.codex

import com.hazelcast.core.HazelcastInstance
import com.openlattice.hazelcast.HazelcastQueue
import com.openlattice.scheduling.RunnableTask

class ScheduledMessageTask(
        val message: MessageRequest
) : RunnableTask {

    override fun run(hazelcastInstance: HazelcastInstance) {
        val twilioQueue = HazelcastQueue.TWILIO.getQueue(hazelcastInstance)
        twilioQueue.offer(message)
        while (twilioQueue.contains(message)) {
            Thread.sleep(5_000)
        }
    }
}