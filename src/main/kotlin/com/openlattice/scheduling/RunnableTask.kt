package com.openlattice.scheduling

import com.hazelcast.core.HazelcastInstance

interface RunnableTask {

    fun run(hazelcastInstance: HazelcastInstance)
}