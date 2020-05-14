package com.openlattice.codex

import com.fasterxml.jackson.annotation.JsonProperty
import com.hazelcast.core.HazelcastInstance
import com.openlattice.client.serialization.SerializationConstants
import com.openlattice.hazelcast.HazelcastQueue
import com.openlattice.scheduling.RunnableTask

data class SendCodexMessageTask(
        @JsonProperty( SerializationConstants.MESSAGE ) val message: MessageRequest
) : RunnableTask {

    override fun run(hazelcastInstance: HazelcastInstance) {
        val twilioQueue = HazelcastQueue.TWILIO.getQueue(hazelcastInstance)
        twilioQueue.add(message)

        // The twilio queue is not persistent. This sleep call is intended to ensure the message has been processed and
        // sent before the method returns and the scheduled task is deleted. Twilio rate limits us to 1 message / second
        // so we check in increments of processing time for a batch of 5 messages to reduce the number of network calls.
        while (twilioQueue.contains(message)) {
            Thread.sleep(5_000)
        }
    }

}